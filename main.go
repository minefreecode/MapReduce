package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== Паттерн Map Reduce исполненное в конкурентном режиме ===")

	data := []string{
		"hello world",
		"hello go",
		"world of concurrency",
		"go programming",
	}
	fmt.Printf("Введённые данные: %v\n", data)
	mapped := generateKeyValueChan(data)
	grouped := createMapFromChan(mapped)
	result := sumMapValue(grouped)
	fmt.Println("\nПодсчёт количества слов:")
	for word, count := range result {
		fmt.Printf(" %s: %d\n", word, count)
	}

	fmt.Println("\nВыполнение программы завершено")

}

// KeyValue Пара ключ-значение
type KeyValue struct {
	Key   string
	Value int
}

// Разделяет текст на пары ключ-значение и размещает пары KeyValue в каналах
func generateKeyValueChan(data []string) <-chan KeyValue {
	out := make(chan KeyValue, len(data)*10) // Каналы

	var wg sync.WaitGroup       //Инициализация группы ожидания
	for _, line := range data { //Цикл по переданным данным
		wg.Add(1)              //Увеличить счётчик линий, поскольку в горутине обрабатываем линию
		go func(text string) { //Функцию выполняем в виде горутины
			defer wg.Done()                                //Уменьшаем счётчик группы ожиданий когда горутина выполнена
			words := strings.Fields(strings.ToLower(text)) //Разделяем линию на слова
			for _, word := range words {                   //Цикл по словам линии
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond) //Задержка
				out <- KeyValue{Key: word, Value: 1}                        //Добавляет пару ключ-значение в канад
				fmt.Printf("Map: выпущено (%s, 1)\n", word)                 //Информативное сообщение
			}
		}(line) //Запуск горутины
	}

	//Горутина нужна чтобы закрывать выходной канал когда группа ожидания достигает счётчика 0
	go func() {
		wg.Wait()  //Ожидание когда счётчик 0
		close(out) //Закрытие канала
	}()

	return out //Возвращение канала
}

// Перетасовка пар ключ-значение
func createMapFromChan(mapped <-chan KeyValue) map[string][]int {
	grouped := make(map[string][]int) //Иницициализация типа Map c ключом string и значениями массива []int
	var mu sync.Mutex                 //Мьютекс для блокировки

	var wg sync.WaitGroup    // Группа блокировки
	for kv := range mapped { //Цикл данным канала
		wg.Add(1)              //Увеличиваем счётчик для горутины
		go func(kv KeyValue) { //Запуск горутины
			defer wg.Done()                                                              //Уменьшение счётчика после сделанного дела
			mu.Lock()                                                                    //Блокировка кода
			grouped[kv.Key] = append(grouped[kv.Key], kv.Value)                          //В Map добавляем значения
			mu.Unlock()                                                                  //Разблокировка кода
			fmt.Printf("Перетасовка: сгруппировано %s -> %v\n", kv.Key, grouped[kv.Key]) // Информативное сообщение по ключу и значению
		}(kv)
	}

	wg.Wait()      //Ожидание когда группа ожидания достигнет 0
	return grouped //Возврат новой группировки
}

// Просуммировать значение Map
func sumMapValue(grouped map[string][]int) map[string]int {
	result := make(map[string]int) //Инициализировать Map
	var mu sync.Mutex              //Мьютекс для блокировки общего кода горутин

	var wg sync.WaitGroup               // Инициализация группы ожидания
	for word, counts := range grouped { //Цикл по значениям Map
		wg.Add(1)                            // Увеличиваем счётчик горутин
		go func(word string, counts []int) { //Запускаем горутину
			defer wg.Done()                                              //Умменьшение счётчика горутин когда горутина выполнилась
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond) //Задержка времени выполнения

			total := 0                     //Начальная сумма
			for _, count := range counts { //Суммирование значений в массиве
				total += count
			}
			mu.Lock()            //Блокировка общего кода для всех горутин
			result[word] = total //Присваивание значения общему результату
			mu.Unlock()          //Разблокировка мьютекса
			fmt.Printf("Просуммировано: %s -> %d\n", word, total)
		}(word, counts)
	}

	wg.Wait() //Ожидание когда счётчик горутин опустится до 0
	return result
}
