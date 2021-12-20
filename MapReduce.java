import java.io.File;
import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Semaphore;

abstract class MapReduce {
	public static final boolean DEBUG = false;

	private String InputPath;
	private String OutputPath;
	private int nfiles;
	int count = 1;
	int seconPart = 0;
	boolean printReducer = true;

	// Split Statistics (Por cada thread)
	int split_numInputFiles = 0; // (Global) Numero de ficheros leidos
	int split_TotalbytesReaded = 0; // Numero total de bytes leidos
	int split_TotalnumLinesReaded = 0; // Numero de lineas leidas
	int split_TotalnumTuples = 0; // Numero de tuplas de entrada generadas

	// Map Statistics (Por cada thread)
	int map_numInputTuples = 0; // Numero de tuplas de entrada procesadas
	int map_bytesProcessed = 0; // Numero de bytes procesados
	int map_numOutputTuples = 0; // Numero de tuplas de salida generadas

	// Suffle Statistics
	int suffle_numOutputTuples = 0;
	int suffle_numKeys = 0;

	// Reducer Statistics
	int reduce_numKeys = 0; // Numero de claves diferentes procesadas
	int reduce_numOccurences = 0; // Numero de ocurrencias procesadas
	float reduce_averageOccurKey = 0; // Valor medio ocurrencias/clave
	int reduce_numOutputBytes = 0; // Numero bytes escritos de salida

	Semaphore sem;
	Semaphore semReduce;

	private final Lock lock = new ReentrantLock();
	private Vector<Map> Mappers = new Vector<Map>();
	private Vector<Reduce> Reducers = new Vector<Reduce>();

	public MapReduce() {
		SetInputPath("");
		SetOutputPath("");
	}

	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de
	// directorio/fichero entrada
	// y directorio salida se inicilizan mediante Set* y las funciones Map y reduce
	// sobreescribiendo los
	// métodos abstractos.

	public MapReduce(String input, String output, int nReducers) {
		SetInputPath(input);
		SetOutputPath(output);
		SetReducers(nReducers);
	}

	private void AddMap(Map map) {
		Mappers.add(map);
	}

	private void AddReduce(Reduce reducer) {
		Reducers.add(reducer);
	}

	public void SetInputPath(String path) {
		InputPath = path;
	}

	public void SetOutputPath(String path) {
		OutputPath = path;
	}

	public void SetReducers(int nReducers) {
		for (int x = 0; x < nReducers; x++) {
			AddReduce(new Reduce(this, OutputPath + "/result.r" + (x + 1)));
		}
	}

	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge,
	// reduce.
	public Error Run() {

		Thread thr1[];
		File folder = new File(InputPath);
		File[] listOfFiles = folder.listFiles();
		thr1 = new Thread[listOfFiles.length];
		Map map[] = new Map[listOfFiles.length];
		nfiles = listOfFiles.length;
		System.out.println("\n\u001B[32m-> Number of input files: " + nfiles + "\033[0m");

		// if (Split(InputPath) != Error.COk)
		// Error.showError("MapReduce::Run-Error Split");
		sem = new Semaphore(1);
		semReduce = new Semaphore(1);
		if (folder.isDirectory()) {
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					System.out.println("Input path " + listOfFiles[i].getPath());
					try {
						map[i] = new Map(this);
						thr1[i] = new Thread(
								new Fases_Concurentes_1(map[i], listOfFiles[i].getAbsolutePath()));
						thr1[i].start();
					} catch (RuntimeException e) { // Fin //
						Error.showError("Error Create thread " + e + " fase 1");
					}
				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}
			System.out.println("\n\u001B[32mProcesando Fase 1 con " + nfiles + " threads...\033[0m");
			for (int i = 0; i < listOfFiles.length; i++) {
				try {
					// System.out.println("\n\u001B[32m Thread Acabado\033[0m");
					thr1[i].join();
				} catch (InterruptedException e) { // Fin //
					Error.showError("Error Join thread " + e + " fase 1");
				}
			}
		} else {
			thr1 = new Thread[1];
			thr1[0] = new Thread(new Fases_Concurentes_1(new Map(this), folder.getAbsolutePath()));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			thr1[0].start();
			try {
				thr1[1].join();
			} catch (InterruptedException e) { // Fin //
				Error.showError("Error Join thread " + e + " fase 1");
			}
		}
		// if (Maps() != Error.COk)
		// Error.showError("MapReduce::Run-Error Map");

		// if (Suffle() != Error.COk)
		// Error.showError("MapReduce::Run-Error Merge");
		/*
		 * Thread thr[];
		 * thr = new Thread[Reducers.size()];
		 * System.out.println("\n\u001B[32mProcesando Fase 2 con " + Reducers.size() +
		 * " threads...\033[0m");
		 * 
		 * for (int i = 0; i < Reducers.size(); i++) {
		 * try {
		 * thr[i] = new Thread(new Fases_Concurentes_2(i));
		 * thr[i].start();
		 * } catch (RuntimeException e) { // Fin //
		 * Error.showError("Error Create thread " + e + " fase 2");
		 * }
		 * }
		 * 
		 * for (int i = 0; i < Reducers.size(); i++) {
		 * try {
		 * thr[i].join();
		 * } catch (InterruptedException e) { // Fin //
		 * Error.showError("Error Join thread " + e + " fase 2");
		 * }
		 * }
		 */
		// if (Reduces() != Error.COk)
		// Error.showError("MapReduce::Run-Error Reduce");
		System.out.println("\n");
		return (Error.COk);
	}

	public class Fases_Concurentes_1 implements Runnable {
		Map map;
		String splitPath;

		public Fases_Concurentes_1(Map map, String splitPath) {
			this.map = map;
			this.splitPath = splitPath;
		}

		@Override
		public void run() {

			// **** Split *****
			// System.out.println("Split: thread " + Thread.currentThread().getId());
			if (Split(this.splitPath, this.map) != Error.COk) {
				Error.showError("MapReduce::Split run error");
			}

			// System.out.println(this.map.PrintSplit());
			printTotalSplit(this.map);

			// **** Map *****
			// System.out.println("Map: thread " + Thread.currentThread().getId());
			if (Maps(this.map) != Error.COk) {
				Error.showError("MapReduce::Maps run error");
			}

			// System.out.println(this.map.PrintMap());
			printTotalMap(this.map);

			synchronized (Mappers) {
				AddMap(this.map);
			}

			try {
				sem.acquire();
			} catch (Exception e) {
				System.out.println(e);
			}
			// lock.lock();
			if (seconPart < Reducers.size()) {
				// try{
				// sem.acquire();
				seconPart++;
				// lock.unlock();
				sem.release();
				// System.out.print("Esto es el Count: "+count);
				// **** Suffle *****
				// System.out.println("Suffle: thread " + Thread.currentThread().getId());
				if (Suffle() != Error.COk) {
					Error.showError("MapReduce::Suffle run error");
				}
				// System.out.println(Reducers.get(0).PrintSuffle());

				printTotalSuffle();

				// **** Reducer ****
				// System.out.println("Reduce: thread " + Thread.currentThread().getId());
				if (Reduces() != Error.COk) {
					Error.showError("MapReduce::Reduce run error");
				}
				printTotalReduce(seconPart);

			} else {
				// lock.unlock();
				sem.release();
			}
		}
	}

	/*
	 * public class Fases_Concurentes_2 implements Runnable {
	 * int num_reducer;
	 * 
	 * public Fases_Concurentes_2(int num_reducer) {
	 * this.num_reducer = num_reducer;
	 * }
	 * 
	 * @Override
	 * public void run() {
	 * System.out.println("Reduces: thread " + Thread.currentThread().getId());
	 * if (Reduces(num_reducer) != Error.COk)
	 * Error.showError("MapReduce::Reduce run error");
	 * }
	 * }
	 */

	// Genera y lee diferentes splits: 1 split por fichero.
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	private Error Split(String input, Map map) {
		if (map.ReadFileTuples(input) != Error.COk)
			Error.showError("MapReduce::Split run error");
		return (Error.COk);
	}

	// Ejecuta cada uno de los Maps.
	private Error Maps(Map map) {
		if (map.Run() != Error.COk)
			Error.showError("MapReduce::Map Run error.\n");
		return (Error.COk);
	}

	public Error Map(Map map, MapInputTuple tuple) {
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return (Error.CError);
	}

	public synchronized int printTotalSplit(Map map) {

		this.split_TotalbytesReaded += map.GetSplit_bytesReaded();
		this.split_TotalnumLinesReaded += map.GetSplit_numLinesReaded();
		this.split_TotalnumTuples += map.GetSplit_numTuples();
		if (count < nfiles) {
			lock.lock();
			count++;
			lock.unlock();
			// System.out.println("Count: " + count + " de nfiles:" + nfiles);
			System.out.println(map.PrintSplit());
			try {
				wait();
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println(map.PrintSplit());
			System.out.println("Total Split  -> NTotalArchvios:" + nfiles + "   \tbytesReaded:"
					+ this.split_TotalbytesReaded + "  \tnumLinesReaded:" + this.split_TotalnumLinesReaded
					+ "  \tnumTuples:" + this.split_TotalnumTuples + "\n");
			notifyAll();
			count = 1;
		}
		return 0;
	}

	public synchronized int printTotalMap(Map map) {
		this.map_numInputTuples += map.GetMap_numInputTuples();
		this.map_bytesProcessed += map.GetMap_bytesProcessed();
		this.map_numOutputTuples += map.GetMap_numOutputTuples();
		if (count < nfiles) {
			lock.lock();
			count++;
			lock.unlock();
			// System.out.print("MAP Contador: " + count + "\t");
			// System.out.println("Count: " + count + " de nfiles:" + nfiles);
			System.out.println(map.PrintMap());
			try {
				wait();
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println(map.PrintMap());
			System.out.println("Total Map  -> NTotalMaps" + nfiles + "   \tInputTuples:" + this.map_numInputTuples
					+ "  \tBytesReader:" + this.map_bytesProcessed + "  \tOutputTuples:" + this.map_numOutputTuples
					+ "\n");
			notifyAll();
			count = 0;
		}
		return 0;
	}

	public synchronized int printTotalSuffle() {

		if (count < Reducers.size() - 1) {
			lock.lock();
			count++;
			lock.unlock();
			suffle_numOutputTuples += Reducers.get(count).Getsuffle_numOutputTuples();
			suffle_numKeys += Reducers.get(count).Getsuffle_numKeys();
			System.out.println(Reducers.get(count).PrintSuffle());
			// System.out.println("Count: " + count + " de nfiles:" + nfiles);
			try {
				wait();
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println(Reducers.get(count).PrintSuffle());
			suffle_numOutputTuples += Reducers.get(count).Getsuffle_numOutputTuples();
			suffle_numKeys += Reducers.get(count).Getsuffle_numKeys();
			System.out.println("Suffle Total  -> numOutputTuples: " + suffle_numOutputTuples + " tnumProcessedKeys: "
					+ suffle_numKeys + "\n");
			notifyAll();
			count = 1;
		}
		return 0;
	}

	public synchronized int printTotalReduce(int nreducers) {
		if (count < nreducers) {
			lock.lock();
			count++;
			lock.unlock();
			// System.out.print("MAP Contador: " + count + "\t");
			// System.out.println("Count: " + count + " de nfiles:" + nfiles);
			try {
				wait();
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println(
					"Reduce Total -> numKeys: " + reduce_numKeys + " numOccurences: " + reduce_numOccurences
							+ " averageOccurencesPerKey " + reduce_averageOccurKey / nreducers
							+ " numOutputBytes " + reduce_numOutputBytes);
			notifyAll();
		}
		return 0;
	}

	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de
	// hash como
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle() {

		while (Mappers.size() != 0) {
			Map m;
			synchronized (Mappers) {
				m = Mappers.get(Mappers.size() - 1);
				Mappers.remove(Mappers.size() - 1);
			}
			for (String key : m.GetOutput().keySet()) {
				// Calcular a que reducer le corresponde está clave:
				// System.out.println("key.hashCode() " + key +" "+ key.hashCode() + "
				// Reducers.size() " + Reducers.size() + " >>>> "+
				// Thread.currentThread().getId());
				// System.out.println("REDUCERRRRRRRRRR " + Reducers.size());
				int r = Math.abs(key.hashCode()) % Reducers.size();
				// r no puede ser negativo, ya que el numero de reducer son 0, 1, 2, 3...
				if (MapReduce.DEBUG) { //
					System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);
					// System.out.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce
					// " + r + " >>>> " + Thread.currentThread().getId());
					// Añadir todas las tuplas de la clave al reducer correspondiente.
				}
				// synchronized(Reducers.get(r)){
				Reducers.get(r).AddInputKeys(key, m.GetOutput().get(key));
				// }
			}
			m.GetOutput().clear();
		}

		return (Error.COk);
	}

	private Error Reduces() {

		try {
			semReduce.acquire();
		} catch (Exception e) {
			System.out.println(e);
		}
		while (Reducers.size() != 0) {
			Reduce red;
			synchronized (Reducers) {
				red = Reducers.get(Reducers.size() - 1);
				Reducers.remove(Reducers.size() - 1);
			}
			semReduce.release();
			if (red.Run() != Error.COk)
				Error.showError("MapReduce::Reduce Run error.\n");
			lock.lock();
			System.out.println(red.PrintReducer());
			reduce_numKeys += red.Getreduce_numKeys();
			reduce_numOccurences += red.Getreduce_numOccurences();
			reduce_averageOccurKey += red.Getreduce_averageOccurKey();
			reduce_numOutputBytes += red.Getreduce_numOutputBytes();
			lock.unlock();
		}
		return (Error.COk);
	}

	/*
	 * private Error Suffle(Map map) {
	 * 
	 * for (String key : map.GetOutput().keySet()) {
	 * // Calcular a que reducer le corresponde está clave:
	 * // System.out.println("key.hashCode() " + key +" "+ key.hashCode() + "
	 * // Reducers.size() " + Reducers.size() + " >>>> "+
	 * // Thread.currentThread().getId());
	 * 
	 * int r = Math.abs(key.hashCode()) % Reducers.size();
	 * 
	 * // r no puede ser negativo, ya que el numero de reducer son 0, 1, 2, 3...
	 * 
	 * if (MapReduce.DEBUG) { //
	 * System.err.println("DEBUG::MapReduce::Suffle merge key " + key +
	 * " to reduce " + r);
	 * // System.out.println("DEBUG::MapReduce::Suffle merge key " + key + " to
	 * reduce
	 * // " + r + " >>>> " + Thread.currentThread().getId());
	 * // Añadir todas las tuplas de la clave al reducer correspondiente.
	 * }
	 * Reducers.get(r).AddInputKeys(key, map.GetOutput().get(key));
	 * }
	 * map.GetOutput().clear();
	 * return (Error.COk);
	 * }
	 */

	// Ejecuta cada uno de los Reducers.
	/*
	 * private Error Reduces(int num_reducer) {
	 * if (Reducers.get(num_reducer).Run() != Error.COk)
	 * Error.showError("MapReduce::Reduce Run error.\n");
	 * return (Error.COk);
	 * }
	 */

	public Error Reduce(Reduce reduce, String key, Collection<Integer> values) {
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return (Error.CError);
	}
}
