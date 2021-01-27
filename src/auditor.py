from decouple import config

from src.commands import (
    SQL_TABELAS_MONITORAR,
    SQL_BASE_TRIGGER,
    SQL_IF_UPDATE_TRIGGER,
    SQL_TABELA_AUDITORIA,
    SQL_TABELA_AUDITORIA_TABELAS,
    SQL_PROCEDURES_SP_REMOTE_USER,
    SQL_PROCEDURES_SP_CAMPO_TABELA,
    SQL_PROCEDURES_SP_PK_TABELA,
    SQL_PROCEDURES_SP_TABELAS_AUDITADAS,
    SQL_PROCEDURES_SP_INSERT_AUDITORIA, SQL_BASE_EXECUTE_INSERT_AUDITORIA
)
from src.wfb import FirebirdConnector


class Auditor:

    def __init__(self):
        db_data = config('DATABASE_DATA')
        user_data = config('USER_DATA')
        pass_data = config('PASS_DATA')

        db_log = config('DATABASE_LOG')
        user_log = config('USER_LOG')
        pass_log = config('PASS_LOG')

        self.objetos = dict(
            tabelas=[],
            triggers=dict()
        )
        self.data_connection = FirebirdConnector(db_data, user=user_data, password=pass_data)
        self.log_connection = FirebirdConnector(db_log, user=user_log, password=pass_log)

    def _tabelas(self):
        self.objetos['tabelas'] = self.data_connection.get(SQL_TABELAS_MONITORAR)

    def _statement_update(self, table, campo, pk):
        return (
            SQL_IF_UPDATE_TRIGGER
                .replace('%execute%', SQL_BASE_EXECUTE_INSERT_AUDITORIA)
                .replace('%campo%', campo)
                .replace('%p1%', '1')
                .replace('%p2%', f"CAST(new.{pk} AS VARCHAR(300))")
                .replace('%p3%', f"'{table}'")
                .replace('%p4%', f"'{campo}'")
                .replace('%p5%', f"old.{campo}")
                .replace('%p6%', f"new.{campo}")
        )

    def _prepare_trigger(self, nome, tabela, pk):
        trigger = (
            SQL_BASE_TRIGGER
                .replace('%TRIGGER_NAME%', nome)
                .replace('%TRIGGER_TABLE%', tabela)
                .replace('%TRIGGER_PK%', pk)
        )
        fields = [i['campo'] for i in self.objetos.get('tabelas', []) if i.get('tabela') == tabela]

        updates = []

        for f in fields:
            updates.append(self._statement_update(tabela, f, pk))

        return trigger.replace('%UPDATE%', ''.join(updates))

    def mount_triggers(self):
        self._tabelas()
        for t in self.objetos.get('tabelas', []):
            tabela = t.get("tabela", '').strip()
            pk = t.get('pk', '').strip()
            trigger_name = f'TG_{tabela}_AUDIT'
            if self.objetos['triggers'].get(trigger_name, None) is None:
                self.objetos['triggers'][trigger_name] = self._prepare_trigger(trigger_name, tabela, pk)

        for k in self.objetos['triggers']:
            sql = self.objetos['triggers'][k]
            try:
                self.data_connection.connection.execute_immediate(sql)
                self.data_connection.connection.commit()
            except Exception as e:
                f = open(f"{k}.sql", "w")
                f.write(sql)
                f.close()
                print(f'Falha ao criar a trigger {k} {e}')

    def prepare_base(self):
        try:
            self.log_connection.connection.execute_immediate(SQL_TABELA_AUDITORIA)
            self.log_connection.connection.execute_immediate(SQL_TABELA_AUDITORIA_TABELAS)
            self.data_connection.connection.commit()
        except Exception as e:
            print('Tabelas já criadas. ' + e.args[0])

        try:
            self.data_connection.connection.execute_immediate(SQL_PROCEDURES_SP_REMOTE_USER)
            self.data_connection.connection.execute_immediate(SQL_PROCEDURES_SP_CAMPO_TABELA)
            self.data_connection.connection.execute_immediate(SQL_PROCEDURES_SP_PK_TABELA)
            self.data_connection.connection.execute_immediate(SQL_PROCEDURES_SP_TABELAS_AUDITADAS)
            self.data_connection.connection.execute_immediate(SQL_PROCEDURES_SP_INSERT_AUDITORIA)
            self.data_connection.connection.commit()
        except Exception as e:
            print('Probleminha. ' + e.args[0] + SQL_PROCEDURES_SP_REMOTE_USER)
