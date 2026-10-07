use std::thread;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};

fn main() {
    //clase_tres();
    //clase_cuatro();

    // clase 6
    //joinhandle();
    // scope();
    //mpsc();
}

fn clase_cuatro(){
    //hands_on();
    //hands_on_scope();
    //semaforo();
    //barrera();
}

fn hands_on(){
    // demostrar como varios hilos incrementan un contador compartido
    // utilizando mutex para la exclusion mutua y arc para gestionar 
    // el ciclo de vida de la memoria compartida de forma segura

    let contador = Arc::new(Mutex::new(0));
    let mut hilos = vec![];

    for _ in 0..10{
        let clone = Arc::clone(&contador);
        let hilo = thread::spawn(move || {
            let mut num = clone.lock().unwrap();
            *num +=1;
        });
        hilos.push(hilo);
    }

    for hilo in hilos {
        hilo.join().unwrap();
    }

    println!("Hands on, valor final: {}", *contador.lock().unwrap());
}

fn hands_on_scope(){
    let  contador = Mutex::new(0);

    thread::scope(|s| {
        for _ in 0..10{
            let referencia = &contador;
            s.spawn(move || {
                let mut num = referencia.lock().unwrap();
                *num += 1;
            });
        }
    });

    println!("Resultado scope: {}", *contador.lock().unwrap());
}

#[tokio::main]
async fn semaforo(){
    // un estacionamiento tiene 3 lugares disponible. Generar 10 hilos
    // que intenten ingresar al mismo tiempo.
    // Iniciar un semaforo en 3. Cada hilo debe intentar
    // adquirir el recurso (wait), imprimir "Auto X estacionado",
    // simular un tiempo de espera de 1 segundo, imprimir
    // "Auto X sale" y liberar el recurso (Signal)

    // inicializo un semaforo con 3 espacios.
    let semaforo = Arc::new(Semaphore::new(3));
    let mut tareas = vec![];

    for auto in 0..10{
        let clone = Arc::clone(&semaforo);

        // spawn lanza un hilo nivel SO.
        let tarea = tokio::spawn(async move {
            // quiero el permiso, espero hasta obtenerlo.
            let permiso = clone.acquire().await.unwrap();
            println!("Auto {} entró al estacionamiento", auto);
            sleep(Duration::from_secs(2)).await;
            println!("Auto {} salió del estacionamiento", auto);

            // dropeo el permiso.
            drop(permiso)
        });
        tareas.push(tarea);
    }

    for tarea in tareas{
        tarea.await.unwrap();
    }
}

#[tokio::main]
async fn barrera(){
    // imponer orden de ejecucion estricto entre hilos con ciclos
    // de vida independientes.

    // Cocinero, camarero. El camarero no puede despachar si el plato
    // no esta listo.

    let semaforo = Arc::new(Semaphore::new(0));

    let scam = Arc::clone(&semaforo);
    let camarero = tokio::spawn(async move{
        // quiere tomar el permiso, espera hasta tener una señal
        let permiso = scam.acquire().await.unwrap();
        println!("Camarero: Plato servido");
    });

    let sco = Arc::clone(&semaforo);
    let cocinero = tokio::spawn(async move{
        sleep(Duration::from_secs(2)).await;
        println!("Cocinero: Plato listo");
        // le da la señal que hay un lugar disponible
        sco.add_permits(1);
    });

    tokio::join!(camarero, cocinero);
}

fn clase_tres(){
    // clase 3. El error es safety. No pueden acceder simultaneamente a
    // un recurso compartido, la sc.

    // solucion:
    let  contador = Arc::new(Mutex::new(0));
    
    let c1 = Arc::clone(&contador);
    let h1 = thread::spawn(move || {
        let mut lock = c1.lock().unwrap();
        *lock += 1; 
    }); 
    
    let c2 = Arc::clone(&contador);
    let h2 = thread::spawn(move||{ 
        let mut lock = c2.lock().unwrap();
        *lock +=1;
    }); 
    
    h1.join().unwrap();
    h2.join().unwrap(); 

    println!("Clase 3, Valor final: {}", *contador.lock().unwrap())
}


// dado un vector de numeros enteros implementar un mecanismo que
// divida el trabajo en 2 subtareas paralelas para calcular la
// suma total. Resuelve el problema utilizando al menos algún mecanismo
// de sincronización o abstracción del lenguaje (ej: mpsc, retornos
// mediante JoinHandle o thread::scope)

// canales
fn mpsc(){
    let numeros: Vec<i32> = (1..=100).collect();
    let mitad = numeros.len() / 2;

    let (parte1, parte2) = numeros.split_at(mitad);
    let p1 = parte1.to_vec();
    let p2 = parte2.to_vec();

    let (tx, rx) = channel();

    let tx1 = tx.clone();
    thread::spawn(move ||{
        let suma: i32 = p1.iter().sum();
        tx1.send(suma).unwrap();
    });

    thread::spawn(move ||{
        let suma: i32 = p2.iter().sum();
        tx.send(suma).unwrap();
    });

    println!("Canales: {}", rx.recv().unwrap() + rx.recv().unwrap());

}


fn scope(){
    let numeros: Vec<i32> = (1..=100).collect();
    let mitad = numeros.len() / 2;

    let (parte1, parte2) = numeros.split_at(mitad);

    let (subtotal1, subtotal2) = thread::scope(|s| {
        // los hilos toman las referencias &parte1, &parte2 en vez del ownership
        let h1 = s.spawn(|| -> i32{ parte1.iter().sum()});
        let h2 = s.spawn(|| -> i32{parte2.iter().sum()});

        (h1.join().unwrap(), h2.join().unwrap())
    });

    println!("Scope: {}", subtotal1 + subtotal2);

}

fn joinhandle(){
    let numeros: Vec<i32> = (1..=100).collect();
    let mitad = numeros.len() / 2;

    let (parte1, parte2) = numeros.split_at(mitad);
    let parte1 = parte1.to_vec();
    let parte2 = parte2.to_vec();

    let handle1 = thread::spawn(move || -> i32 {
        parte1.iter().sum()
    });

    let handle2 = thread::spawn(move || -> i32{
        parte2.iter().sum()
    });

    let total = handle1.join().unwrap() + handle2.join().unwrap();
    println!("Por Join-Handle: {}", total);

}
