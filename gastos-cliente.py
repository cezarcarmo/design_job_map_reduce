from pyspark import SparkConf, SparkContext

# Define o Spark Context, pois o job será executado via linha de comando com o spark-submit
conf = SparkConf().setMaster("local").setAppName("GastosPorCliente")
sc = SparkContext(conf = conf)

# Função de mapeamento que separa cada um dos campos no dataset
def MapCliente(line):
    campos = line.split(',')
    return (int(campos[0]), float(campos[2]))

# Leitura do dataset a partir do HDFS
input = sc.textFile("hdfs://clientes/gastos-cliente.csv")
mappedInput = input.map(MapCliente)

# Operação de redução por chave para calcular o total gasto por cliente
totalPorCliente = mappedInput.reduceByKey(lambda x, y: x + y)

# Imprime o resultado
resultados = totalPorCliente.collect();
for resultado in resultados:
    print(resultado)
