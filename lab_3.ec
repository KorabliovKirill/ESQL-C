#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* ----------------------------------------- */
/* ГЛАВНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ПОДКЛЮЧЕНИЯ */
/* ----------------------------------------- */
exec SQL begin declare section;
    char db_name[50];
    char user[50];
    char password[50];
exec SQL end declare section;

/* ----------------------------------------- */
/* CONNECT DB */
/* ----------------------------------------- */
void ConnectDB()
{
    strcpy(db_name, "students");
    strcpy(user, "pmi-b2713");
    strcpy(password, "l9C80!DaN");
    printf("Подключение к БД \"%s\"...\n", db_name);
    exec SQL connect to :db_name user :user using :password;
    if (sqlca.sqlcode < 0)
    {
        printf("Ошибка подключения! код %d: %s\n",
               sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
    printf("Подключение к схеме \"pmib2713\"...\n");
    exec SQL set search_path to pmib2713;
    if (sqlca.sqlcode < 0)
    {
        printf("Ошибка подключения к схеме! код %d: %s\n",
               sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
}

/* ----------------------------------------- */
/* DISCONNECT DB */
/* ----------------------------------------- */
void DisconnectDB()
{
    printf("Отключение от БД \"%s\"...\n", db_name);
    exec SQL disconnect :db_name;
    if (sqlca.sqlcode < 0)
    {
        printf("Ошибка отключения! код %d: %s\n",
               sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
}

/* ----------------------------------------- */
/* MENU */
/* ----------------------------------------- */
void PrintMenu()
{
    printf("1) Задача 1\n");
    printf("2) Задача 2\n");
    printf("3) Задача 3\n");
    printf("4) Задача4\n");
    printf("5) Задача 5\n");
    printf("0) Выход из программы\n");
}

/* ----------------------------------------- */
/* TASK 1 */
/* ----------------------------------------- */
void Task1()
{
    exec SQL begin declare section;
        int cnt;
    exec SQL end declare section;

    printf("Задача 1: Выдать число поставок, выполненных для изделий с деталями зеленого цвета.\n");
    exec SQL begin work;

    exec SQL
        select count(*) into :cnt
        from spj
        where n_izd in (
            select distinct spj.n_izd
            from spj
            join p on spj.n_det = p.n_det
            where p.cvet = 'Зеленый'
        );

    if (sqlca.sqlcode < 0)
    {
        printf("Задача 1: Ошибка! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL rollback work;
        return;
    }
    printf("Задача 1: Успех! Число поставок: %d\n", cnt);
    exec SQL commit work;
}

/* ----------------------------------------- */
/* TASK 2 */
/* ----------------------------------------- */
void Task2()
{
    printf("Задача 2: Поменять местами города, где размещены изделия с самым коротким и самым длинным названием...\n");
    exec SQL begin work;

    exec SQL
        update j
        set town = case
            when length(name) = (select min(length(name)) from j) then
                (select town from j where length(name) = (select max(length(name)) from j) order by town limit 1)
            when length(name) = (select max(length(name)) from j) then
                (select town from j where length(name) = (select min(length(name)) from j) order by town limit 1)
            else town
        end
        where length(name) in (
            (select min(length(name)) from j),
            (select max(length(name)) from j)
        );

    if (sqlca.sqlcode < 0)
    {
        printf("Задача 2: Ошибка! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL rollback work;
        return;
    }
    printf("Задача 2: Успех! Изменено строк: %d\n", sqlca.sqlerrd[2]);
    exec SQL commit work;
}

/* ----------------------------------------- */
/* TASK 3 */
/* ----------------------------------------- */
void Task3()
{
    exec SQL begin declare section;
        char n_det[6];
    exec SQL end declare section;

    printf("Задача 3: Найти детали, имеющие поставки, вес которых меньше среднего веса поставок этой детали для изделий из Лондона.\n");

    exec SQL declare curs_parts cursor for
        select distinct spj.n_det
        from spj
        join p on p.n_det = spj.n_det
        where (spj.kol * p.ves) < (
            select avg(spj2.kol * p2.ves)
            from spj spj2
            join j j2 on j2.n_izd = spj2.n_izd
            join p p2 on p2.n_det = spj2.n_det
            where spj2.n_det = spj.n_det
              and j2.town = 'Лондон'
        );

    exec SQL begin work;
    exec SQL open curs_parts;

    if (sqlca.sqlcode < 0)
    {
        printf("Задача 3: Ошибка открытия курсора! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL rollback work;
        return;
    }

    exec SQL fetch curs_parts into :n_det;
    if (sqlca.sqlcode == 100)
    {
        printf("Задача 3: Нет Деталей.\n");
        exec SQL close curs_parts;
        exec SQL commit work;
        return;
    }
    if (sqlca.sqlcode < 0)
    {
        printf("Задача 3: Ошибка в получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL close curs_parts;
        exec SQL rollback work;
        return;
    }

    printf("n_det\n");
    printf("%s\n", n_det);
    int rowcount = 1;

    while (1)
    {
        exec SQL fetch curs_parts into :n_det;
        if (sqlca.sqlcode == 100) break;
        if (sqlca.sqlcode < 0)
        {
            printf("Задача 3: Ошибка в получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
            break;
        }
        printf("%s\n", n_det);
        rowcount++;
    }

    exec SQL close curs_parts;
    printf("Задача 3: Успех! Строк: %d\n", rowcount);
    exec SQL commit work;
}

/* ----------------------------------------- */
/* TASK 4 */
/* ----------------------------------------- */
void Task4()
{
    exec SQL begin declare section;
        char n_post[6];
    exec SQL end declare section;

    printf("Задача 4: Выбрать поставщиков, не поставляющих ни одной из деталей, поставляемых поставщиками, находящимися в Лондоне.\n");

    exec SQL declare curs_sup cursor for
        select s.n_post
        from s
        except
        select distinct spj.n_post
        from spj
        where spj.n_det in (
            select distinct spj.n_det
            from spj
            where spj.n_post in (
                select s.n_post
                from s
                where s.town = 'Лондон'
            )
        );

    exec SQL begin work;
    exec SQL open curs_sup;

    if (sqlca.sqlcode < 0)
    {
        printf("Задача 4: Ошибка открытия курсора! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL rollback work;
        return;
    }

    exec SQL fetch curs_sup into :n_post;
    if (sqlca.sqlcode == 100)
    {
        printf("Задача 4: Поставщиков не найдено.\n");
        exec SQL close curs_sup;
        exec SQL commit work;
        return;
    }
    if (sqlca.sqlcode < 0)
    {
        printf("Задача 4: Ошибка получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL close curs_sup;
        exec SQL rollback work;
        return;
    }

    printf("n_post\n");
    printf("%s\n", n_post);
    int rowcount = 1;

    while (1)
    {
        exec SQL fetch curs_sup into :n_post;
        if (sqlca.sqlcode == 100) break;
        if (sqlca.sqlcode < 0)
        {
            printf("Задача 4: Ошибка получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
            break;
        }
        printf("%s\n", n_post);
        rowcount++;
    }

    exec SQL close curs_sup;
    printf("Задача 4: Успех! Строк: %d\n", rowcount);
    exec SQL commit work;
}

/* ----------------------------------------- */
/* TASK 5 */
/* ----------------------------------------- */
void Task5()
{
    exec SQL begin declare section;
        char n_post[6];
        char name[20];
        int reiting;
        char town[20];
    exec SQL end declare section;

    printf("Задача 5: Выдать полную информацию о поставщиках, выполнивших поставки ТОЛЬКО с объемом от 200 до 500 деталей.\n");

    exec SQL declare curs_only200_500 cursor for
        select s.n_post, s.name, s.reiting, s.town
        from s
        join spj on spj.n_post = s.n_post
        group by s.n_post, s.name, s.reiting, s.town
        having min(spj.kol) >= 200
           and max(spj.kol) <= 500;

    exec SQL begin work;
    exec SQL open curs_only200_500;

    if (sqlca.sqlcode < 0)
    {
        printf("Задача 5: Ошибка открытия курсора! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL rollback work;
        return;
    }

    exec SQL fetch curs_only200_500 into :n_post, :name, :reiting, :town;
    if (sqlca.sqlcode == 100)
    {
        printf("Задача 5: Нет поставщиков!\n");
        exec SQL close curs_only200_500;
        exec SQL commit work;
        return;
    }
    if (sqlca.sqlcode < 0)
    {
        printf("Задача 5: Ошибка получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL close curs_only200_500;
        exec SQL rollback work;
        return;
    }

    printf("|n_post|name               |reiting|town                |\n");
    printf("|%.6s|%-20s|%7d|%-20s|\n", n_post, name, reiting, town);
    int rowcount = 1;

    while (1)
    {
        exec SQL fetch curs_only200_500 into :n_post, :name, :reiting, :town;
        if (sqlca.sqlcode == 100) break;
        if (sqlca.sqlcode < 0)
        {
            printf("Задача 5: Ошибка получения! код %d: %s\n", sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
            break;
        }
        printf("|%.6s|%-20s|%7d|%-20s|\n", n_post, name, reiting, town);
        rowcount++;
    }

    exec SQL close curs_only200_500;
    printf("Задача 5: Успех! Строк: %d\n", rowcount);
    exec SQL commit work;
}

/* ----------------------------------------- */
/* MAIN */
/* ----------------------------------------- */
int main()
{
    ConnectDB();
    while (true)
    {
        printf("\nЧто выполнить?\n");
        PrintMenu();
        printf("Выберите номер: ");
        int number;
        if (scanf("%d", &number) != 1)
        {
            /* сброс ввода, если введено не число */
            int c;
            while ((c = getchar()) != '\n' && c != EOF) ;
            printf("Invalid input, try again.\n");
            continue;
        }
        switch (number)
        {
            case 1: Task1(); break;
            case 2: Task2(); break;
            case 3: Task3(); break;
            case 4: Task4(); break;
            case 5: Task5(); break;
            case 0:
                DisconnectDB();
                return 0;
            default:
                printf("Выберите из доступных номеров!\n");
                break;
        }
    }
}  