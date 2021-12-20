/* ---------------------------------------------------------------
Práctica 4.
Código fuente: Map.java
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

import java.io.*;
import java.util.List;
import java.util.Vector;
import com.google.common.collect.*;

class MapInputTuple {
	long Key;
	String Value;

	public MapInputTuple(long key, String value) {
		setKey(key);
		setValue(value);
	}

	public long getKey() {
		return (Key);
	}

	public void setKey(long key) {
		Key = key;
	}

	public String getValue() {
		return (Value);
	}

	public void setValue(String value) {
		Value = value;
	}
}

public class Map {
	private MapReduce mapReduce;
	private Vector<MapInputTuple> Input = new Vector<MapInputTuple>();
	private ListMultimap<String, Integer> Output = Multimaps
			.synchronizedListMultimap(ArrayListMultimap.<String, Integer>create());

	// Split Statistics (Por cada thread)
	int split_numInputFiles = 0; // (Global) Numero de ficheros leidos
	String fichero = "";
	int split_bytesReaded = 0; // Numero total de bytes leidos
	int split_numLinesReaded = 0; // Numero de lineas leidas
	int split_numTuples = 0; // Numero de tuplas de entrada generadas

	// Map Statistics (Por cada thread)
	int map_numInputTuples = 0; // Numero de tuplas de entrada procesadas
	int map_bytesProcessed = 0; // Numero de bytes procesados
	int map_numOutputTuples = 0; // Numero de tuplas de salida generadas

	public Map(MapReduce mapr) {
		mapReduce = mapr;
	}

	public ListMultimap<String, Integer> GetOutput() {
		return (Output);
	}

	public void PrintOutputs() {
		for (String key : Output.keySet()) {
			List<Integer> ocurrences = Output.get(key);
			System.out.println("Map " + this + " Output: key: " + key + " -> " + ocurrences);
		}
	}

	// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map
	// en forma de
	// tuplas (key,value).
	synchronized public Error ReadFileTuples(String fileName) {
		long Offset = 0;
		FileInputStream fis;
		try {
			fis = new FileInputStream(fileName);
			this.fichero = fileName;
		} catch (FileNotFoundException e) {
			System.err.println("Map::ERROR File " + fileName + " not found.");
			e.printStackTrace();
			return (Error.CErrorOpenInputFile);
		}
		// System.out.println("***** Input Stream Creado **** -->" +
		// Thread.currentThread().getId());
		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				// System.out.println( " # # linea: --> "+ line + " >>>> " +
				// Thread.currentThread().getId());
				// System.err.println("DEBUG::Map input " + Offset + " -> " + line);
				AddInput(new MapInputTuple(Offset, line));
				Offset += line.length();
				split_numLinesReaded = split_numLinesReaded + 1;
				split_bytesReaded = split_bytesReaded + line.length();
			}
		} catch (IOException e) {
			System.err.println("Map::ERROR Reading file " + fileName + ".");
			e.printStackTrace();
			return (Error.CErrorReadingFile);
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			return (Error.CErrorReadingFile);
		}
		return (Error.COk);
	}

	synchronized public void AddInput(MapInputTuple tuple) {
		split_numTuples++;
		Input.add(tuple);
	}

	// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una
	// de ellas
	// invoca a la función de Map especificada por el programador.
	public Error Run() {
		Error err;

		while (!Input.isEmpty()) {
			if (MapReduce.DEBUG)
				System.err.println(
						"DEBUG::Map process input tuple " + Input.get(0).getKey() + " -> " + Input.get(0).getValue());
			err = mapReduce.Map(this, Input.get(0));
			map_numInputTuples += 1;
			if (err != Error.COk)
				return (err);

			Input.remove(0);
		}

		return (Error.COk);
	}

	// Función para escribir un resultado parcial del Map en forma de tupla
	// (key,value)
	public void EmitResult(String key, int value) {
		if (MapReduce.DEBUG)
			System.err.println(
					"DEBUG::Map emit result " + key + " -> " + value + " >>>> " + Thread.currentThread().getId());
		byte[] array1 = key.getBytes();
		map_bytesProcessed += array1.length;
		map_numOutputTuples += 1;
		Output.put(key, new Integer(value));
	}

	public int GetSplit_bytesReaded() {
		return split_bytesReaded;
	}

	public int GetSplit_numLinesReaded() {
		return split_numLinesReaded;
	}

	public int GetSplit_numTuples() {
		return split_numTuples;
	}

	public int GetMap_numInputTuples() {
		return map_numInputTuples;
	}

	public int GetMap_bytesProcessed() {
		return map_bytesProcessed;
	}

	public int GetMap_numOutputTuples() {
		return map_numOutputTuples;
	}

	public String PrintSplit() {
		// printf("Split -> Thread:%ld ConArchivo:%s \tbytesReaded:%i
		// \tnumLinesReaded:%i \tnumTuples:%i \n",
		String print = "Split -> Thread:" + Thread.currentThread().getId()
				+ " bytesReaded:" + this.split_bytesReaded + " numLinesReaded:" + this.split_numLinesReaded
				+ " numTuples:" + this.split_numTuples;
		return print;
	}

	public String PrintMap() {
		// printf("Split -> Thread:%ld ConArchivo:%s \tbytesReaded:%i
		// \tnumLinesReaded:%i \tnumTuples:%i \n",
		String print = "Map -> Thread:" + Thread.currentThread().getId()
				+ " InputTuples:" + this.map_numInputTuples + " BytesReader:" + this.map_bytesProcessed
				+ " OutputTuples:" + this.map_numOutputTuples;
		return print;
	}

}
