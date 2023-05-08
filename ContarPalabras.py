from pyspark import SparkContext
import sys
import string

def word_split(line):
    for c in string.punctuation+"¿!":
        line = line.replace(c, ' ')
    line = line.lower()
    return line.split()

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(filename)

        words_rdd = data.map(lambda x: len(x.split()))
        print('Resultado', words_rdd.sum())
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Faltan argumentos: python3 {sys.argv[0]} <file>")
    else:
        main(sys.argv[1])
