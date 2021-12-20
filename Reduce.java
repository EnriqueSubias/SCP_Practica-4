
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

class Reduce {
	private MapReduce mapReduce;
	PrintWriter OutputFile;
	private ListMultimap<String, Integer> Input = Multimaps
			.synchronizedListMultimap(ArrayListMultimap.<String, Integer>create());

	// Suffle Statiscis
	int suffle_numOutputTuples = 0; // Numero de tuplas de salida procesadas
	int suffle_numKeys = 0; // Numero de claves procesadas

	// Reduce Statiscis
	int reduce_numKeys = 0; // Numero de claves diferentes procesadas
	int reduce_numOccurences = 0; // Numero de ocurrencias procesadas
	float reduce_averageOccurKey = 0; // Valor medio ocurrencias/clave
	int reduce_numOutputBytes = 0; // Numero bytes escritos de salida

	// Constructor para una tarea Reduce, se le pasa la función que reducción que
	// tiene que
	// ejecutar para cada tupla de entrada y el nombre del fichero de salida en
	// donde generará
	// los resultados.

	public Reduce(MapReduce mapr, String OutputPath) {
		mapReduce = mapr;
		try {
			OutputFile = new PrintWriter(OutputPath);
			System.out.println("DEBUG::Creating output file " + OutputPath);
		} catch (FileNotFoundException e) {
			System.err.println("Reduce::ERROR Open output file " + OutputPath + ".");
			e.printStackTrace();
		}
	}

	// Finish: cierra fichero salida.
	public void Finish() {
		OutputFile.close();
	}

	// Función para añadir las tuplas de entrada para la función de redución en
	// forma de lista de
	// tuplas (key,value).
	public synchronized void AddInputKeys(String key, Collection<Integer> values) {
		for (Integer value : values)
			AddInput(key, value);
		suffle_numKeys++;
	}

	private synchronized void AddInput(String key, Integer value) {
		if (MapReduce.DEBUG)
			System.err.println("DEBUG::Reduce add input " + key + "-> " + value);
		suffle_numOutputTuples++;
		Input.put(key, value);
	}

	// Función de ejecución de la tarea Reduce: por cada tupla de entrada invoca a
	// la función
	// especificada por el programador, pasandolo el objeto Reduce, la clave y la
	// lista de
	// valores.
	public Error Run() {
		Iterator<String> keyIterator = Input.keySet().iterator();
		while (keyIterator.hasNext()) {
			String key = keyIterator.next();

			Error err = mapReduce.Reduce(this, key, Input.get(key));
			if (err != Error.COk)
				return (err);
			keyIterator.remove();
		}
		Finish();
		reduce_averageOccurKey = (float) (reduce_numKeys) / (float) (reduce_numOccurences);
		return (Error.COk);
	}

	// Función para escribir un resulta en el fichero de salida.
	public void EmitResult(String key, int value) {
		// System.out.println("Write in document: " + key + " >>>> " +
		// Thread.currentThread().getId());
		OutputFile.write(key + " " + value + "\n");
		byte[] array1 = key.getBytes();
		reduce_numOutputBytes += array1.length;
		reduce_numKeys++;
		reduce_numOccurences += value;
	}

	public String PrintSuffle() {
		// printf("Split -> Thread:%ld ConArchivo:%s \tbytesReaded:%i
		// \tnumLinesReaded:%i \tnumTuples:%i \n",
		String print = "Suffle  -> numOutputTuples: " + suffle_numOutputTuples + " tnumProcessedKeys: "
				+ suffle_numKeys;
		return print;
	}

	public String PrintReducer() {
		String print = "Reduce  -> numKeys: " + reduce_numKeys + " numOccurences: " + reduce_numOccurences
				+ " averageOccurencesPerKey " + reduce_averageOccurKey + " numOutputBytes " + reduce_numOutputBytes;
		return print;
	}

	public int Getsuffle_numOutputTuples() {
		return suffle_numOutputTuples;
	}

	public int Getsuffle_numKeys() {
		return suffle_numKeys;
	}

	public int Getreduce_numKeys() {
		return reduce_numKeys;
	}

	public int Getreduce_numOccurences() {
		return reduce_numOccurences;
	}

	public float Getreduce_averageOccurKey() {
		return reduce_averageOccurKey;
	}

	public int Getreduce_numOutputBytes() {
		return reduce_numOutputBytes;
	}

}
