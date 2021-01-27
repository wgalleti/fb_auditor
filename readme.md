# Gerador de auditoria

 - Criar arquivo de configuração `.env`

```
DATABASE_DATA=127.0.0.1:path_db_data
USER_DATA=sysdba
PASS_DATA=masterkey

DATABASE_LOG=127.0.0.1:path_db_log
USER_LOG=sysdba
PASS_LOG=masterkey
```

- Executar o seguinte script
```python
from ..src.auditor import Auditor
a = Auditor()
a.prepare_base()
# INSERIR NA TABELA AUDITORIA_TABELAS AS TABELAS QUE SERÃO AUDITADAS
a.log_connection.connection.execute_immediate("insert into auditoria_tabelas (tabela) values ('CLIENTE')")
a.log_connection.connection.execute_immediate("insert into auditoria_tabelas (tabela) values ('PRODUTO')")
a.log_connection.connection.commit()
a.mount_triggers()
```

- Altere ou remova ou insira um registro das tabelas auditada
- Verifique a tabela `AUDITORIA` na base de log

> Agradecimentos ao [Edson Gregório](https://mqfs.com.br/)
