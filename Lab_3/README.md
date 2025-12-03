# Взаимодействие программ на C и СУБД PostgreSQL

## Описание программы

Программа подключается к учебной базе данных `students`, переходит в схему `pmib2713` и предоставляет интерактивное меню для выполнения пяти задач по запросам к классической базе SPP (Поставщики — Детали — Изделия — Поставки).

Все SQL-запросы реализованы максимально близко к чистому SQL, без лишних промежуточных вычислений в C-коде, с использованием современных возможностей PostgreSQL (коррелированные подзапросы, `EXCEPT`, агрегатные функции в `HAVING` и т.д.).

### Структура таблиц

| Таблица | Поля                              |
|---------|-----------------------------------|
| `s`     | n_post, name, reiting, town       |
| `p`     | n_det, name, cvet, ves, town      |
| `j`     | n_izd, name, town                 |
| `spj`   | n_post, n_det, n_izd, kol         |

## Выполняемые задачи

### Задача 1
Выдать число поставок, выполненных для изделий, содержащих хотя бы одну деталь зелёного цвета.

```sql
SELECT COUNT(*)
FROM spj
WHERE n_izd IN (
    SELECT DISTINCT spj.n_izd
    FROM spj
    JOIN p ON spj.n_det = p.n_det
    WHERE p.cvet = 'Зеленый'
);
```

### Задача 2
Поменять местами города у изделий с самым коротким и самым длинным названием (при равенстве длин — берётся город, первый по алфавиту).

```sql
UPDATE j
SET town = CASE
    WHEN length(name) = (SELECT MIN(length(name)) FROM j) THEN
        (SELECT town FROM j WHERE length(name) = (SELECT MAX(length(name)) FROM j) ORDER BY town LIMIT 1)
    WHEN length(name) = (SELECT MAX(length(name)) FROM j) THEN
        (SELECT town FROM j WHERE length(name) = (SELECT MIN(length(name)) FROM j) ORDER BY town LIMIT 1)
    ELSE town
END
WHERE length(name) IN (
    (SELECT MIN(length(name)) FROM j),
    (SELECT MAX(length(name)) FROM j)
);
```

### Задача 3
Найти детали, у которых есть хотя бы одна поставка с общим весом меньше среднего веса поставок этой детали для изделий из Лондона.

```sql
SELECT DISTINCT spj.n_det
FROM spj
JOIN p ON p.n_det = spj.n_det
WHERE (spj.kol * p.ves) < (
    SELECT AVG(spj2.kol * p2.ves)
    FROM spj spj2
    JOIN j j2 ON j2.n_izd = spj2.n_izd
    JOIN p p2 ON p2.n_det = spj2.n_det
    WHERE spj2.n_det = spj.n_det
      AND j2.town = 'Лондон'
);
```

### Задача 4
Выбрать поставщиков, которые не поставляют ни одной детали, поставляемой поставщиками из Лондона.

```sql
SELECT s.n_post
FROM s
EXCEPT
SELECT DISTINCT spj.n_post
FROM spj
WHERE spj.n_det IN (
    SELECT DISTINCT spj.n_det
    FROM spj
    WHERE spj.n_post IN (
        SELECT s.n_post
        FROM s
        WHERE s.town = 'Лондон'
    )
);
```

### Задача 5
Выдать полную информацию о поставщиках, которые выполняли поставки ТОЛЬКО в количестве от 200 до 500 деталей включительно (все их поставки в этом диапазоне, и хотя бы одна есть).

```sql
SELECT s.n_post, s.name, s.reiting, s.town
FROM s
JOIN spj ON spj.n_post = s.n_post
GROUP BY s.n_post, s.name, s.reiting, s.town
HAVING MIN(spj.kol) >= 200
   AND MAX(spj.kol) <= 500;
```

## Особенности реализации

- Явные транзакции (`BEGIN WORK` / `COMMIT WORK` / `ROLLBACK WORK`)
- Полная обработка ошибок через `sqlca.sqlcode`
- Использование курсоров для всех запросов, возвращающих множество строк
- Защита от некорректного ввода в меню
- Красивый табличный вывод результатов
- Подключение к базе захардкожено (учебный вариант)

## Сборка и запуск

```bash
# 1. Прекомпиляция ESQL/C
pgcci main

# 3. Запуск
./main.exe
```