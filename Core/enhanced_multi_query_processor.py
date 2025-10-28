#
# ENHANCED_MULTI_QUERY_PROCESSOR.py - VERSIONE FINALE CON FIX TYPE CONVERSION
# Copyright 2025 TIM SPA
# Author Daniele Speziale
# FIX: Type conversion da Oracle a SQL Server, gestione overflow
#
import logging
import pandas as pd
import warnings
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, Any, Tuple, List
from pathlib import Path
from Core.database_manager import DatabaseManager

warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


class MultitaskingQueryProcessor:
    """Processore multitasking - OTTIMIZZATO E ROBUSTO"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_manager = DatabaseManager(config)
        self.logger = logging.getLogger(__name__)
        self.batch_size = min(config['execution']['batch_size'], 5000)
        self.drop_existing = config['execution']['drop_existing_tables']
        self.queries = config.get('queries', [])

        self.max_workers = config.get('execution', {}).get('max_workers', 4)
        self.max_concurrent_queries = config.get('execution', {}).get('max_concurrent_queries', 3)
        self.query_timeout = config.get('execution', {}).get('query_timeout_seconds', 300)

        self.progress_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.progress_queue = Queue()

        self.query_directory = Path(config.get('execution', {}).get('query_directory', 'queries'))
        self.reports_query_directory = Path(
            config.get('execution', {}).get('reports_query_directory', 'reports/queries'))
        self.reports_directory = Path(config.get('execution', {}).get('reports_directory', 'reports'))

        self._setup_directories()

    def _setup_directories(self):
        """Crea tutte le directory necessarie se non esistono"""
        directories = [
            ('queries', self.query_directory),
            ('reports_queries', self.reports_query_directory),
            ('reports', self.reports_directory)
        ]

        for dir_name, dir_path in directories:
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"SETUP: Directory {dir_name} creata: {dir_path}")

    def _thread_safe_log(self, level: str, message: str):
        """Logging thread-safe"""
        with self.log_lock:
            if level.upper() == 'INFO':
                self.logger.info(message)
            elif level.upper() == 'ERROR':
                self.logger.error(message)
            elif level.upper() == 'WARNING':
                self.logger.warning(message)
            elif level.upper() == 'DEBUG':
                self.logger.debug(message)

    def _get_query_directory_for_type(self, query_config: Dict[str, Any]) -> Path:
        """Determina la directory corretta in base al tipo di query"""
        query_type = query_config.get('query_type', 'standard')
        return self.reports_query_directory if query_type == 'report' else self.query_directory

    def resolve_sql_query(self, query_config: Dict[str, Any]) -> str:
        """Risolve la query SQL da diverse sorgenti (inline, array, file, template)"""
        query_name = query_config.get('name', 'unnamed')
        query_type = query_config.get('query_type', 'standard')

        try:
            if 'sql' in query_config and isinstance(query_config['sql'], list):
                sql = ' '.join(line.strip() for line in query_config['sql'])
                self._thread_safe_log('INFO',
                                      f"QUERY: [{query_name}] ({query_type}) risolto da array multiriga ({len(query_config['sql'])} righe)")
                return sql

            elif 'sql' in query_config and isinstance(query_config['sql'], str):
                self._thread_safe_log('INFO', f"QUERY: [{query_name}] ({query_type}) risolto da stringa inline")
                return query_config['sql']

            elif 'sql_file' in query_config:
                sql_filename = query_config['sql_file']
                query_dir = self._get_query_directory_for_type(query_config)
                sql_file_path = query_dir / sql_filename

                if not sql_file_path.exists():
                    raise FileNotFoundError(f"File SQL non trovato: {sql_file_path}")

                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    sql = f.read()

                self._thread_safe_log('INFO', f"QUERY: [{query_name}] ({query_type}) risolto da file {sql_file_path}")
                return sql

            elif 'sql_template' in query_config:
                template_filename = query_config['sql_template']
                template_params = query_config.get('template_params', {})
                query_dir = self._get_query_directory_for_type(query_config)
                template_file_path = query_dir / template_filename

                if not template_file_path.exists():
                    raise FileNotFoundError(f"Template SQL non trovato: {template_file_path}")

                with open(template_file_path, 'r', encoding='utf-8') as f:
                    template_content = f.read()

                sql = template_content.format(**template_params)
                self._thread_safe_log('INFO',
                                      f"QUERY: [{query_name}] ({query_type}) risolto da template {template_file_path} con {len(template_params)} parametri")
                return sql

            else:
                raise ValueError("Nessuna sorgente SQL valida trovata (sql, sql_file, o sql_template)")

        except Exception as e:
            self._thread_safe_log('ERROR', f"ERRORE: Risoluzione SQL [{query_name}] ({query_type}): {e}")
            raise

    def _get_sql_source_type(self, query_config: Dict[str, Any]) -> str:
        """Determina il tipo di sorgente SQL per il reporting"""
        if 'sql' in query_config:
            if isinstance(query_config['sql'], list):
                return "array_multiriga"
            else:
                return "inline_string"
        elif 'sql_file' in query_config:
            query_type = query_config.get('query_type', 'standard')
            return f"file_{query_type}"
        elif 'sql_template' in query_config:
            return "template_con_parametri"
        else:
            return "sconosciuto"

    def execute_single_query(self, query_config: Dict[str, Any], thread_id: int) -> Dict[str, Any]:
        """Esegue una singola query in un thread separato"""
        query_name = query_config['name']
        source_db = query_config['source_database']
        dest_db = query_config['destination_database']
        dest_table = query_config['destination_table']
        dest_schema = query_config.get('destination_schema', 'dbo')
        query_type = query_config.get('query_type', 'standard')

        start_time = time.time()

        try:
            if not query_config.get('enabled', True):
                self._thread_safe_log('INFO',
                                      f"THREAD-{thread_id}: SKIP Query [{query_name}] ({query_type}) disabilitata")
                return {
                    'success': False,
                    'query_name': query_name,
                    'reason': 'disabled',
                    'thread_id': thread_id,
                    'execution_time': 0
                }

            sql_query = self.resolve_sql_query(query_config)

            if not sql_query.strip():
                raise ValueError(f"Query SQL vuota per [{query_name}]")

            self._thread_safe_log('INFO',
                                  f"THREAD-{thread_id}: AVVIO Query {query_name} ({query_type}) da [{source_db}]")

            with self.db_manager.get_connection(source_db) as conn:
                df = pd.read_sql(sql_query, conn)

            if df.empty:
                self._thread_safe_log('WARNING',
                                      f"THREAD-{thread_id}: Query [{query_name}] ({query_type}) ha restituito 0 righe")
                return {
                    'success': True,
                    'query_name': query_name,
                    'rows': 0,
                    'source': source_db,
                    'destination': dest_db,
                    'table': f"{dest_schema}.{dest_table}",
                    'sql_type': self._get_sql_source_type(query_config),
                    'query_type': query_type,
                    'thread_id': thread_id,
                    'execution_time': time.time() - start_time
                }

            df.columns = [self._clean_column_name(col) for col in df.columns]

            # ✅ FIX: Normalizza i dati prima di scrivere
            df = self._normalize_dataframe(df, thread_id)

            self._write_to_destination(df, dest_db, dest_table, dest_schema, query_name, query_type, thread_id)

            full_table_name = f"{dest_schema}.{dest_table}"
            execution_time = time.time() - start_time

            self._thread_safe_log('INFO',
                                  f"THREAD-{thread_id}: OK Query {query_name} ({query_type}) completata: {len(df)} righe -> [{dest_db}].[{full_table_name}] in {execution_time:.2f}s")

            return {
                'success': True,
                'query_name': query_name,
                'rows': len(df),
                'source': source_db,
                'destination': dest_db,
                'table': full_table_name,
                'sql_type': self._get_sql_source_type(query_config),
                'query_type': query_type,
                'thread_id': thread_id,
                'execution_time': execution_time,
                'dataframe': df
            }

        except Exception as e:
            execution_time = time.time() - start_time
            self._thread_safe_log('ERROR', f"THREAD-{thread_id}: ERRORE Query [{query_name}]: {e}")
            return {
                'success': False,
                'query_name': query_name,
                'error': str(e),
                'thread_id': thread_id,
                'execution_time': execution_time
            }

    def _normalize_dataframe(self, df: pd.DataFrame, thread_id: int) -> pd.DataFrame:
        """
        ✅ FIX: Normalizza dati da Oracle per SQL Server
        - Converti float che dovrebbero essere int
        - Gestisci overflow (172.0 → 172)
        - Pulisci valori NULL
        """
        self._thread_safe_log('DEBUG',
                              f"THREAD-{thread_id}: NORMALIZZAZIONE dati (righe: {len(df)}, colonne: {len(df.columns)})")

        for col in df.columns:
            try:
                # Se è float ma tutti i valori sono interi → converti a int
                if pd.api.types.is_float_dtype(df[col]):
                    # Controlla se sono tutti numeri interi
                    non_null = df[col].dropna()

                    if len(non_null) > 0 and all(x == int(x) for x in non_null):
                        # Converti float → int (evita overflow)
                        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)
                        self._thread_safe_log('DEBUG',
                                              f"THREAD-{thread_id}: Colonna [{col}] convertita FLOAT → BIGINT")

                # Gestisci string vuote → NULL
                if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: None if (pd.isna(x) or x == '' or str(x).strip() == '') else x)

                # Gestisci valori infiniti
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].replace([float('inf'), float('-inf')], None)

            except Exception as e:
                self._thread_safe_log('WARNING',
                                      f"THREAD-{thread_id}: Errore normalizzazione colonna [{col}]: {e}")

        return df

    def _write_to_destination(self, df: pd.DataFrame, dest_db: str, dest_table: str,
                              dest_schema: str, query_name: str, query_type: str, thread_id: int):
        """Scrive DataFrame nel database di destinazione"""
        self.db_manager.create_schema_if_not_exists(dest_db, dest_schema)

        with self.db_manager.get_connection(dest_db) as dest_conn:
            full_table_name = f"[{dest_schema}].[{dest_table}]"
            cursor = dest_conn.cursor()

            try:
                if self.drop_existing:
                    try:
                        cursor.execute(
                            f"IF OBJECT_ID('{full_table_name}', 'U') IS NOT NULL DROP TABLE {full_table_name}")
                        dest_conn.commit()
                        self._thread_safe_log('INFO',
                                              f"THREAD-{thread_id}: DROP Tabella {full_table_name} eliminata")
                    except Exception as e:
                        self._thread_safe_log('WARNING',
                                              f"THREAD-{thread_id}: WARNING Impossibile eliminare {full_table_name}: {e}")

                dest_config = self.db_manager.get_database_config(dest_db)

                if dest_config.get('type') == 'mssql':
                    self._write_to_sqlserver_optimized(cursor, dest_conn, df, dest_table, dest_schema,
                                                       full_table_name, query_name, thread_id)
                else:
                    df.to_sql(
                        name=dest_table,
                        con=dest_conn,
                        schema=dest_schema,
                        if_exists='replace' if self.drop_existing else 'append',
                        index=False,
                        chunksize=self.batch_size,
                        method='multi'
                    )

                self._thread_safe_log('INFO',
                                      f"THREAD-{thread_id}: CREATE Tabella {full_table_name} creata ({len(df)} righe)")

            finally:
                cursor.close()

    def _infer_column_type_safe(self, series: pd.Series) -> str:
        """
        ✅ FIX: Inferenza tipo colonna SICURA per SQL Server
        Gestisce conversioni da Oracle, overflow, NULL
        """
        try:
            # Filtra NULL per analisi
            non_null = series.dropna()

            if len(non_null) == 0:
                # Tutti NULL → string di default
                return "NVARCHAR(MAX)"

            # Se è numerato (int/float)
            if pd.api.types.is_numeric_dtype(series):
                # Controlla range di valori
                min_val = non_null.min()
                max_val = non_null.max()

                # ✅ FIX: Usa BIGINT per valori che potrebbero essere float
                if min_val >= -9223372036854775808 and max_val <= 9223372036854775807:
                    return "BIGINT"
                else:
                    # Overflow → usa FLOAT
                    return "FLOAT"

            # Se è datetime
            if pd.api.types.is_datetime64_any_dtype(series):
                return "DATETIME2"

            # Se è string
            if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                # Calcola lunghezza massima
                max_len = series.astype(str).str.len().max()
                max_len = max(min(max_len, 4000), 50)
                return f"NVARCHAR({max_len})"

            # Default
            return "NVARCHAR(MAX)"

        except Exception as e:
            self._thread_safe_log('WARNING', f"Errore inferenza tipo: {e}")
            return "NVARCHAR(MAX)"

    def _build_create_table_ddl_safe(self, full_table_name: str, df: pd.DataFrame) -> str:
        """Costruisce DDL SICURA con type inference robusta"""
        column_definitions = []

        for col in df.columns:
            col_type = self._infer_column_type_safe(df[col])
            column_definitions.append(f"[{col}] {col_type}")

        return f"CREATE TABLE {full_table_name} ({', '.join(column_definitions)})"

    def _write_to_sqlserver_optimized(self, cursor, conn, df: pd.DataFrame, dest_table: str,
                                      dest_schema: str, full_table_name: str,
                                      query_name: str, thread_id: int):
        """Inserimento ottimizzato e sicuro su SQL Server"""
        if df.empty:
            self._thread_safe_log('WARNING', f"THREAD-{thread_id}: DataFrame vuoto per {full_table_name}")
            return

        start_time = time.time()

        # Pulisci colonne
        clean_columns = []
        for col in df.columns:
            clean_col = str(col).replace(' ', '_').replace('-', '_').replace('.', '_')
            clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
            if not clean_col or clean_col[0].isdigit():
                clean_col = f"col_{clean_col}"
            clean_columns.append(clean_col)
        df.columns = clean_columns

        # ✅ FIX: Usa type inference SICURA
        create_sql = self._build_create_table_ddl_safe(full_table_name, df)
        cursor.execute(create_sql)
        conn.commit()

        self._thread_safe_log('DEBUG', f"THREAD-{thread_id}: Tabella creata con DDL sicuro")

        # Prepara dati - ultra-veloce
        insert_cols = f"({', '.join([f'[{col}]' for col in df.columns])})"
        placeholders = ", ".join(["?" for _ in df.columns])
        insert_sql = f"INSERT INTO {full_table_name} {insert_cols} VALUES ({placeholders})"

        # Conversione rapida da DataFrame a tuple
        data_tuples = [tuple(row) for row in df.values]

        # Batch size ottimale
        optimal_batch = min(self.batch_size, len(data_tuples), 5000)

        self._thread_safe_log('INFO',
                              f"THREAD-{thread_id}: INIZIO inserimento {len(data_tuples)} righe con batch_size={optimal_batch}")

        rows_inserted = 0
        for i in range(0, len(data_tuples), optimal_batch):
            batch = data_tuples[i:i + optimal_batch]

            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                rows_inserted += len(batch)

                if len(data_tuples) > 10000 and i % (optimal_batch * 10) == 0:
                    pct = (rows_inserted / len(data_tuples)) * 100
                    self._thread_safe_log('DEBUG',
                                          f"THREAD-{thread_id}: {pct:.1f}% ({rows_inserted}/{len(data_tuples)})")

            except Exception as e:
                conn.rollback()
                self._thread_safe_log('ERROR',
                                      f"THREAD-{thread_id}: ERRORE batch [{i}-{i + len(batch)}]: {e}")
                raise

        elapsed = time.time() - start_time
        rate = len(data_tuples) / elapsed if elapsed > 0 else 0

        self._thread_safe_log('INFO',
                              f"THREAD-{thread_id}: ✅ {len(data_tuples)} righe inserite in {elapsed:.2f}s ({rate:.0f} righe/sec)")

    def _clean_column_name(self, col_name: str) -> str:
        """Pulisce nomi colonne per compatibilità SQL Server"""
        cleaned = re.sub(r'[^\w]', '_', str(col_name))
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.strip('_')
        if not cleaned:
            cleaned = 'unnamed_column'
        return cleaned

    def execute_all_queries_multitasking(self) -> Dict[str, Any]:
        """Esegue tutte le query configurate"""
        self._thread_safe_log('INFO', f"AVVIO: Pipeline OTTIMIZZATA (max_workers: {self.max_workers})")

        if not any(self.query_directory.glob('*.sql')) or not any(self.reports_query_directory.glob('*.sql')):
            self.generate_sample_query_files()

        enabled_queries = [q for q in self.queries if q.get('enabled', True)]

        if not enabled_queries:
            self._thread_safe_log('WARNING', "Nessuna query abilitata da eseguire")
            return {
                'executed_queries': {},
                'written_tables': [],
                'errors': ['Nessuna query abilitata'],
                'stats': {'standard_queries': 0, 'report_queries': 0, 'total_rows_processed': 0,
                          'total_execution_time': 0}
            }

        results = {
            'executed_queries': {},
            'written_tables': [],
            'errors': [],
            'stats': {
                'standard_queries': 0,
                'report_queries': 0,
                'total_rows_processed': 0,
                'total_execution_time': 0,
                'concurrent_executions': 0
            }
        }

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="QueryWorker") as executor:
            future_to_query = {
                executor.submit(self.execute_single_query, query_config, i): query_config
                for i, query_config in enumerate(enabled_queries)
            }

            self._thread_safe_log('INFO',
                                  f"SUBMITTED: {len(future_to_query)} query sottomesse per esecuzione parallela")

            completed_count = 0
            for future in as_completed(future_to_query, timeout=self.query_timeout * len(enabled_queries)):
                query_config = future_to_query[future]
                completed_count += 1

                try:
                    result = future.result(timeout=self.query_timeout)

                    if result['success']:
                        query_name = result['query_name']
                        query_type = result.get('query_type', 'standard')

                        results['executed_queries'][query_name] = {
                            'rows': result['rows'],
                            'source': result['source'],
                            'destination': result['destination'],
                            'table': result['table'],
                            'sql_type': result['sql_type'],
                            'query_type': query_type,
                            'thread_id': result['thread_id'],
                            'execution_time': result['execution_time']
                        }

                        if result['rows'] > 0:
                            results['written_tables'].append({
                                'database': result['destination'],
                                'table': result['table'],
                                'query_type': query_type
                            })

                        if query_type == 'report':
                            results['stats']['report_queries'] += 1
                        else:
                            results['stats']['standard_queries'] += 1

                        results['stats']['total_rows_processed'] += result['rows']
                        results['stats']['total_execution_time'] += result['execution_time']

                        self._thread_safe_log('INFO',
                                              f"COMPLETED: {completed_count}/{len(enabled_queries)} - {query_name} ({result['rows']} righe in {result['execution_time']:.2f}s)")

                    else:
                        error_msg = f"Query [{result['query_name']}]: {result.get('error', result.get('reason', 'Unknown error'))}"
                        results['errors'].append(error_msg)
                        self._thread_safe_log('ERROR',
                                              f"FAILED: {completed_count}/{len(enabled_queries)} - {error_msg}")

                except Exception as e:
                    query_name = query_config.get('name', 'unnamed')
                    error_msg = f"Query [{query_name}]: {e}"
                    results['errors'].append(error_msg)
                    self._thread_safe_log('ERROR', f"EXCEPTION: {completed_count}/{len(enabled_queries)} - {error_msg}")

        total_pipeline_time = time.time() - start_time
        results['stats']['total_pipeline_time'] = total_pipeline_time
        results['stats']['concurrent_executions'] = len(enabled_queries)

        stats = results['stats']
        self._thread_safe_log('INFO', f"✅ PIPELINE COMPLETATA: {stats['standard_queries']} query standard, "
                                      f"{stats['report_queries']} query reports, "
                                      f"{stats['total_rows_processed']} righe totali")
        self._thread_safe_log('INFO', f"⚡ PERFORMANCE: {total_pipeline_time:.2f}s pipeline, "
                                      f"speedup: {stats['total_execution_time'] / total_pipeline_time:.1f}x")

        return results

    def generate_sample_query_files(self):
        """Genera file SQL di esempio"""
        standard_queries = {
            "sample_users.sql": """-- Sample User Query\nSELECT TOP 1000 * FROM sys.tables;""",
        }

        report_queries = {
            "daily_activity_report.sql": """-- Daily Activity Report\nSELECT TOP 100 * FROM sys.views;""",
        }

        for filename, content in standard_queries.items():
            file_path = self.query_directory / filename
            if not file_path.exists():
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)

        for filename, content in report_queries.items():
            file_path = self.reports_query_directory / filename
            if not file_path.exists():
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)

    def execute_all_queries(self) -> Dict[str, Any]:
        """Alias per execute_all_queries_multitasking"""
        return self.execute_all_queries_multitasking()

    def get_table_info(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Ottiene informazioni dettagliate su una tabella"""
        try:
            with self.db_manager.get_connection(db_name) as conn:
                cursor = conn.cursor()

                cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
                result = cursor.fetchone()
                row_count = result[0] if result else 0

                table_only = table_name.split('.')[-1].strip('[]')
                cursor.execute(f"""
                    SELECT COLUMN_NAME as name, DATA_TYPE as type
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_only}'
                """)

                columns_result = cursor.fetchall()
                columns = [{'name': col[0], 'type': col[1]} for col in columns_result]
                cursor.close()

                return {
                    'exists': True,
                    'table_name': table_name,
                    'row_count': row_count,
                    'columns': columns
                }

        except Exception as e:
            self._thread_safe_log('ERROR', f"Verifica tabella {table_name} in {db_name}: {e}")
            return {
                'exists': False,
                'table_name': table_name,
                'row_count': 0,
                'columns': [],
                'error': str(e)
            }


# Alias per compatibilità
EnhancedMultiQueryProcessor = MultitaskingQueryProcessor