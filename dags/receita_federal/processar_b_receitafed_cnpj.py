import os
import csv
import io
import zipfile
import requests
import shutil
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp

BASE_URL = "https://arquivos.receitafederal.gov.br"
HDFS_BASE = "hdfs://namenode:9000/dados"

SCHEMAS = {
    "EMPRE": ("empresa", ["cnpj_basico", "nome_empresarial", "natureza_juridica", "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo"]),
    "ESTABELE": ("estabelecimento", ["cnpj_basico","cnpj_ordem","cnpj_dv","matriz_filial","nome_fantasia","situacao","data_situacao","motivo_situicao","cidade_ext","pais","inicio_ativ","cnae_princ","cnae_sec","tipo_lograd","lograd","num","compl","bairro","cep","uf","municipio","ddd1","tel1","ddd2","tel2","ddd_fax","fax","email","sit_especial","data_sit_especial"]),
    "SOCIO": ("socio", ["cnpj_basico","id_socio","nome","cpf_cnpj","qualificacao","data_entrada","pais","rep_legal","nome_rep","qual_rep","faixa_etaria"]),
    "SIMPLES": ("dados_simples", ["cnpj_basico","op_simples","data_op","data_exc","op_mei","data_mei","data_exc_mei"]),
    "CNAE": ("cnae", ["codigo", "descricao"]),
    "MUNIC": ("municipio", ["codigo", "descricao"]),
    "NATJU": ("natureza_juridica", ["codigo", "descricao"]),
    "PAIS": ("pais", ["codigo", "descricao"]),
    "QUALS": ("qualificacao_socio", ["codigo", "descricao"]),
    "MOTI": ("motivos", ["codigo", "descricao"])
}

base_files = ["Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes", "Simples"]
FILES_TO_PROCESS = base_files + [f"Empresas{i}" for i in range(10)] + [f"Estabelecimentos{i}" for i in range(10)] + [f"Socios{i}" for i in range(10)]

file_to_schema = {"Cnaes": "CNAE", "Motivos": "MOTI", "Municipios": "MUNIC", "Naturezas": "NATJU", "Paises": "PAIS", "Qualificacoes": "QUALS", "Simples": "SIMPLES"}
for prefix, code in (("Empresas", "EMPRE"), ("Estabelecimentos", "ESTABELE"), ("Socios", "SOCIO")):
    for i in range(10):
        file_to_schema[f"{prefix}{i}"] = code

def run_hdfs_cmd(spark, args):
    """Mesmas operações do original, usando o Hadoop disponível no PySpark."""
    Path = spark._jvm.org.apache.hadoop.fs.Path
    target = Path(args[-1])
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if args[0] == "-test":
        return bool(fs.globStatus(target))
    if args[0] == "-mkdir":
        if not fs.mkdirs(target):
            raise RuntimeError(f"Não foi possível criar {args[-1]}")
    elif args[0] == "-put":
        local = Path(os.path.abspath(args[-2]))
        fs.copyFromLocalFile(False, True, local, target)
    else:
        raise ValueError(f"Operação HDFS não suportada: {args[0]}")
    return True

def main():
    print("[INFO] Iniciando ingestão dos Dados Abertos da Receita Federal (CNPJ)...")

    spark = (
        SparkSession.builder
        .appName("Ingestao - Receita Federal CNPJ")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.warehouse", "hdfs://namenode:9000/dados")
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.auth_type", "NONE")
        .config("spark.sql.defaultCatalog", "nessie")
        .getOrCreate()
    )

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.b_receitafed_cnpj")

        date_str = os.getenv("RFB_REFERENCE_MONTH") or datetime.now().strftime("%Y-%m")
        reference = datetime.strptime(date_str, "%Y-%m")
        base_dl = f"{os.environ['RFB_BASE_URL'].rstrip('/')}/{date_str}"
        hdfs_dest = f"{HDFS_BASE}/cobre/receitafederal/cnpj/{reference.year}/{reference.month}_extracted"
        print(f"[INFO] Garantindo diretório HDFS: {hdfs_dest}")
        run_hdfs_cmd(spark, ["-mkdir", "-p", hdfs_dest])

        destino_tmp = "/tmp/rfb_cnpj"
        os.makedirs(destino_tmp, exist_ok=True)

        session = requests.Session()
        retry = Retry(total=3, backoff_factor=5, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 1. Download, extração e upload dos CSVs para HDFS
        for fname in FILES_TO_PROCESS:
            key = file_to_schema.get(fname) or next((k for k in SCHEMAS if fname.upper().startswith(k)), None)
            if not key:
                print(f"[WARN] Arquivo {fname} não mapeado em SCHEMAS")
                continue

            table_name, cols = SCHEMAS[key]
            hdfs_file_path = f"{hdfs_dest}/{table_name}_{fname}.csv"

            # Check de existência no HDFS
            if run_hdfs_cmd(spark, ["-test", "-e", hdfs_file_path]):
                print(f"[INFO] Já existe no HDFS: {hdfs_file_path}")
                continue

            local_zip = os.path.join(destino_tmp, f"{fname}.zip")
            local_csv = os.path.join(destino_tmp, f"{fname}.csv")
            url = f"{base_dl}/{fname}.zip"

            print(f"[INFO] Baixando: {fname}.zip ({date_str})")
            try:
                with session.get(url, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    with open(local_zip, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)
            except Exception as e:
                raise RuntimeError(f"Download falhou: {fname}.zip") from None

            try:
                with zipfile.ZipFile(local_zip) as z:
                    src_name = z.namelist()[0]
                    with z.open(src_name) as infile, open(local_csv, 'w', encoding='latin1', newline='') as outfile:
                        outfile.write(";".join(cols) + "\n")
                        text_stream = io.TextIOWrapper(infile, encoding='latin1', errors='ignore')
                        reader = csv.reader(text_stream, delimiter=';', quotechar='"')
                        writer = csv.writer(outfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        for row in reader:
                            writer.writerow(row[:len(cols)])
            except Exception as e:
                print(f"[ERRO] Erro na extração [{type(e).__name__}]: {e}")
                if os.path.exists(local_zip):
                    os.remove(local_zip)
                raise

            # Upload do CSV para HDFS
            print(f"[INFO] Enviando CSV para HDFS: {hdfs_file_path}")
            run_hdfs_cmd(spark, ["-put", "-f", local_csv, hdfs_file_path])

            if os.path.exists(local_zip):
                os.remove(local_zip)
            if os.path.exists(local_csv):
                os.remove(local_csv)

        # 2. Leitura dos CSVs salvos no HDFS e materialização na camada Bronze (Nessie/Iceberg)
        print("[INFO] Iniciando materialização das tabelas Iceberg a partir do HDFS...")
        for key, (table_name, cols) in SCHEMAS.items():
            hdfs_file_pattern = f"{hdfs_dest}/{table_name}_*.csv"

            if not run_hdfs_cmd(spark, ["-test", "-e", hdfs_file_pattern]):
                print(f"[WARN] Nenhum arquivo HDFS encontrado para padrão: {hdfs_file_pattern}")
                continue

            print(f"[INFO] Processando tabela Iceberg {table_name} a partir do HDFS...")
            schema = StructType([StructField(c, StringType(), True) for c in cols])

            df = (
                spark.read
                .option("sep", ";")
                .option("header", "true")
                .option("encoding", "ISO-8859-1")
                .option("escape", "\"")
                .option("multiLine", "true")
                .schema(schema)
                .csv(hdfs_file_pattern)
            )
            df = df.withColumn("dtca", current_timestamp())

            fq_table = f"nessie.b_receitafed_cnpj.{table_name}"

            if table_name == "estabelecimento":
                df = df.repartition("uf").sortWithinPartitions("cnpj_basico")
                df.writeTo(fq_table).using("iceberg").partitionedBy(col("uf")).createOrReplace()
            elif table_name in ("empresa", "socio"):
                df = df.repartition(10, "cnpj_basico").sortWithinPartitions("cnpj_basico")
                df.writeTo(fq_table).using("iceberg").createOrReplace()
            elif table_name == "dados_simples":
                df = df.repartition("op_simples").sortWithinPartitions("cnpj_basico")
                df.writeTo(fq_table).using("iceberg").partitionedBy(col("op_simples")).createOrReplace()
            else:
                df.repartition(5).writeTo(fq_table).using("iceberg").createOrReplace()

            print(f"[OK] Tabela Iceberg '{fq_table}' criada/atualizada com sucesso.")

    finally:
        spark.stop()
        print("[INFO] Processamento finalizado.")

if __name__ == "__main__":
    main()
