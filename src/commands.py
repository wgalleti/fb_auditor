SQL_TABELA_AUDITORIA = """
CREATE TABLE AUDITORIA (
  ID INTEGER GENERATED BY DEFAULT AS IDENTITY,
  PK VARCHAR(100),
  DATA_HORA TIMESTAMP,
  UUSUARIO VARCHAR(300),
  EVENTO CHAR(1),
  TABELA VARCHAR(31),
  CAMPO VARCHAR(31),
  DE VARCHAR(8190),
  PARA VARCHAR(8190),
  CONSTRAINT PK_AUDITORIA PRIMARY KEY (ID)
)
"""

SQL_TABELA_AUDITORIA_TABELAS = """    
CREATE TABLE AUDITORIA_TABELAS (
  ID INTEGER GENERATED BY DEFAULT AS IDENTITY,
  TABELA VARCHAR(100) NOT NULL,
  MONITORA_INSERT INTEGER DEFAULT 0,
  MONITORA_UPDATE INTEGER DEFAULT 0,
  MONITORA_DELETE INTEGER DEFAULT 0,
  CONSTRAINT PK_AUDITORIA_TABELAS PRIMARY KEY (ID)
)
"""

SQL_PROCEDURES_SP_REMOTE_USER = """
CREATE OR ALTER PROCEDURE SP_REMOTE_USER
RETURNS (
  UUSUARIO VARCHAR(1000)
)
AS
BEGIN
  SELECT
    FIRST 1
    MON$REMOTE_OS_USER || ':' ||MON$REMOTE_HOST || '@' || MON$REMOTE_ADDRESS || '/' || MON$REMOTE_PROCESS AS USUARIO
  FROM
    MON$ATTACHMENTS A
  WHERE
    MON$ATTACHMENT_ID = CURRENT_CONNECTION AND
    MON$STATE = 1
  INTO UUSUARIO;
  SUSPEND;
END
"""

SQL_PROCEDURES_SP_CAMPO_TABELA = """
CREATE OR ALTER PROCEDURE SP_CAMPO_TABELA(PTABELA VARCHAR(100))
RETURNS (
  CAMPO VARCHAR(100)
)
AS
BEGIN
  FOR
    SELECT
      TRIM(RF.RDB$FIELD_NAME) AS CAMPO
    FROM 
      RDB$RELATION_FIELDS RF
      JOIN RDB$RELATIONS R ON R.RDB$RELATION_NAME = RF.RDB$RELATION_NAME
    WHERE
      RF.RDB$SYSTEM_FLAG = 0
      AND R.RDB$SYSTEM_FLAG = 0
      AND R.RDB$RELATION_TYPE = 0
      AND R.RDB$RELATION_NAME = :PTABELA
    ORDER BY 
      RF.RDB$RELATION_NAME, 
      RF.RDB$FIELD_POSITION
    INTO CAMPO
  DO
  BEGIN
    SUSPEND;
  END
END
"""

SQL_PROCEDURES_SP_PK_TABELA = """
CREATE OR ALTER PROCEDURE SP_PK_TABELA (PTABELA VARCHAR(100))
RETURNS (
  INDEX_NAME VARCHAR(100),
  FIELD_NAME VARCHAR(100)
)
AS
BEGIN
  FOR
    SELECT
      IX.RDB$INDEX_NAME AS INDEX_NAME,
      SG.RDB$FIELD_NAME AS FIELD_NAME
    FROM
      RDB$INDICES IX
      LEFT JOIN RDB$INDEX_SEGMENTS SG ON IX.RDB$INDEX_NAME = SG.RDB$INDEX_NAME
      LEFT JOIN RDB$RELATION_CONSTRAINTS RC ON RC.RDB$INDEX_NAME = IX.RDB$INDEX_NAME
    WHERE
      RC.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
    AND
      RC.RDB$RELATION_NAME = :PTABELA
    INTO INDEX_NAME, FIELD_NAME
  DO
  BEGIN
    SUSPEND;
  END
END
"""

SQL_PROCEDURES_SP_TABELAS_AUDITADAS = """
CREATE OR ALTER PROCEDURE SP_TABELAS_AUDITADAS
RETURNS (
  TABELA VARCHAR(100)
)
AS
BEGIN
  FOR
    EXECUTE STATEMENT 'SELECT TABELA FROM AUDITORIA_TABELAS'
    ON EXTERNAL '127.0.0.1:D:\HIFUZION\DADOS\SEBBEN\FAZENDA_DADOS\HIFUZION_LOG.FDB'
    AS USER CURRENT_USER PASSWORD 'masterkey'
    INTO TABELA
  DO
  BEGIN
    SUSPEND;
  END
END
"""

SQL_PROCEDURES_SP_INSERT_AUDITORIA = """
CREATE OR ALTER PROCEDURE SP_INSERT_AUDITORIA (
    POPERACAO INTEGER NOT NULL,
    PPK VARCHAR(100) NOT NULL,
    PTABELA VARCHAR(100) NOT NULL,
    PCAMPO VARCHAR(100) NOT NULL,
    POLD VARCHAR(9999) NOT NULL,
    PNEW VARCHAR(9999) NOT NULL)
AS
DECLARE VARIABLE VTIPOOPERACAO VARCHAR(1);
BEGIN
  VTIPOOPERACAO = DECODE(POPERACAO, 0, 'I', 1, 'U', 2, 'D');
  EXECUTE STATEMENT
    ('INSERT INTO AUDITORIA (PK, DATA_HORA, UUSUARIO, EVENTO, TABELA, CAMPO, DE, PARA) VALUES (:P1, :P2, :P3, :P4, :P5, :P6, :P7, :P8)')
    (
        P1 := PPK;
        P2 := CURRENT_TIMESTAMP,
        P3 := (SELECT UUSUARIO FROM SP_REMOTE_USER),
        P4 := VTIPOOPERACAO,
        P5 := PTABELA,
        P6 := PCAMPO,
        P7 := POLD,
        P8 := PNEW
    )
    ON EXTERNAL '127.0.0.1:D:\HIFUZION\DADOS\SEBBEN\FAZENDA_DADOS\HIFUZION_LOG.FDB'
    AS USER CURRENT_USER PASSWORD 'masterkey';
END
"""

SQL_TABELAS_MONITORAR = """
SELECT
  TRIM(RF.RDB$RELATION_NAME) AS TABELA,
  TRIM(RF.RDB$FIELD_NAME) AS CAMPO,
  RDB$GET_CONTEXT('USER_TRANSACTION', 'AUDIT_TABELA_PK') AS PK,
  RDB$SET_CONTEXT(
    'USER_TRANSACTION',
    'AUDIT_TABELA_PK',
    CASE
      WHEN RDB$GET_CONTEXT('USER_TRANSACTION', 'AUDIT_TABELA_PK') IS NULL THEN
        I.RDB$FIELD_NAME
      WHEN RDB$GET_CONTEXT('USER_TRANSACTION', 'AUDIT_TABELA_PK') <> I.RDB$FIELD_NAME THEN
        I.RDB$FIELD_NAME
      ELSE
        RDB$GET_CONTEXT('USER_TRANSACTION', 'AUDIT_TABELA_PK')
    END 
  ) AS PREPARE_PK
FROM 
  RDB$RELATION_FIELDS RF
  JOIN RDB$RELATIONS R ON R.RDB$RELATION_NAME = RF.RDB$RELATION_NAME
  LEFT JOIN RDB$RELATION_CONSTRAINTS RC ON RC.RDB$RELATION_NAME = R.RDB$RELATION_NAME AND RC.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
  LEFT JOIN RDB$INDEX_SEGMENTS I ON I.RDB$INDEX_NAME = RC.RDB$INDEX_NAME AND I.RDB$FIELD_NAME = RF.RDB$FIELD_NAME
WHERE
  RF.RDB$SYSTEM_FLAG = 0
  AND R.RDB$SYSTEM_FLAG = 0
  AND R.RDB$RELATION_TYPE = 0
  AND R.RDB$RELATION_NAME IN (SELECT TABELA FROM SP_TABELAS_AUDITADAS)
ORDER BY 
  RF.RDB$RELATION_NAME, 
  RF.RDB$FIELD_POSITION
"""

SQL_BASE_TRIGGER = """
CREATE OR ALTER TRIGGER %TRIGGER_NAME% 
  FOR %TRIGGER_TABLE%
  ACTIVE AFTER INSERT OR UPDATE OR DELETE 
  POSITION 999
AS
DECLARE VARIABLE vPk VARCHAR(100);
BEGIN
  SELECT FIELD_NAME FROM SP_PK_TABELA('%TRIGGER_TABLE%') INTO vPk;
  
  IF (INSERTING) THEN
  BEGIN 
    EXECUTE STATEMENT
      ('EXECUTE PROCEDURE SP_INSERT_AUDITORIA :P1, :P2, :P3, :P4, :P5, :P6')
      (
        P1 := 0,
        P2 := CAST(NEW.%TRIGGER_PK% AS VARCHAR(300)),
        P3 := '%TRIGGER_TABLE%',
        P4 := :VPK,
        P5 := '',
        P6 := NEW.%TRIGGER_PK%
      ); 
  END 
  
  IF (UPDATING) THEN 
  BEGIN 
    %UPDATE%   
  END

  IF (DELETING) THEN 
  BEGIN 
    EXECUTE STATEMENT
      ('EXECUTE PROCEDURE SP_INSERT_AUDITORIA :P1, :P2, :P3, :P4, :P5, :P6')
      (
        P1 := 2,
        P2 := CAST(OLD.%TRIGGER_PK% AS VARCHAR(300)),
        P3 := '%TRIGGER_TABLE%',
        P4 := :VPK,
        P5 := OLD.%TRIGGER_PK%,
        P6 := ''
      );  
  END
END
"""

SQL_BASE_EXECUTE_INSERT_AUDITORIA = """
        EXECUTE STATEMENT
          ('EXECUTE PROCEDURE SP_INSERT_AUDITORIA :P1, :P2, :P3, :P4, :P5, :P6') 
          (
            P1 := %p1%,
            P2 := %p2%,
            P3 := %p3%,
            P4 := %p4%,
            P5 := %p5%,
            P6 := %p6%
          );"""

SQL_IF_UPDATE_TRIGGER = """
    IF (
        COALESCE(CAST(OLD.%campo% AS VARCHAR(1000)),'') <> 
        COALESCE(CAST(NEW.%campo% AS VARCHAR(1000)), '')
    ) THEN 
    BEGIN
        %execute%
    END
"""
