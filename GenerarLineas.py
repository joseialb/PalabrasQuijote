from pyspark import SparkContext
import sys, random

porcentaje = random.random()*100

def unlines(lista):
    l = ""
    if (lista == []): return l
    for i in lista:
        l += i+"\n"
    return l[:-1]

def main(filename):
	with SparkContext() as sc:
		sc.setLogLevel("ERROR")
		data = sc.textFile(filename)
		dado = random.randint(0, 100)
		if dado <= porcentaje:
			lineas = data.count()
			porCiento = int( (lineas // 100)*porcentaje)
			resultado = data.take(porCiento)
			devuelve = unlines(resultado)
			with open("quijote_s05.txt", "w") as archivo:
				archivo.write(devuelve)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Faltan argumentos: python3 {sys.argv[0]} <file>")
    else:
        main(sys.argv[1])
