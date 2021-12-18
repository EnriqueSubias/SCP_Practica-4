
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;

public class Deadlock {

    static class Friend {
        private final String name;
        public Friend(String name) {
            this.name = name;
        }
        public String getName() {
            return this.name;
        }
        public synchronized void Saludo(Friend bower) {
            System.out.format("%s: %s"
                + "  has bowed to me!%n", 
                this.name, bower.getName());
            bower.DevolverSaludo(this);
        }
        public synchronized void DevolverSaludo(Friend bower) {
            System.out.format("%s: %s"
                + " has bowed back to me!%n",
                this.name, bower.getName());
        }
    }
            

    public static void main(String[] args) {
        final Friend juan =
            new Friend("Juan");
        final Friend pedro =
            new Friend("Pedro");
        new Thread(new Runnable() {
            public void run() { juan.Saludo(pedro); }
        }).start();
        new Thread(new Runnable() {
            public void run() { pedro.Saludo(juan); }
        }).start();
    }
}
