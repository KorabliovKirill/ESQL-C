#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <ecpglib.h>
#include <sqlca.h>

/* ----------------------------------------- */
/* ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ */
/* ----------------------------------------- */

/* Удаление пробелов справа (для CHAR полей из PostgreSQL) */
static void rtrim(char *str)
{
    if (str == NULL) return;
    char *end = str + strlen(str) - 1;
    while (end >= str && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }
}

/* Безопасная очистка буфера ввода (только если там есть данные) */
static void clear_input_buffer(void)
{
    int c;
    while ((c = getchar()) != '\n' && c != EOF);
}

/* Безопасное чтение строки (удаляет \n в конце) */
static int read_line(char *buf, size_t size)
{
    if (fgets(buf, size, stdin) == NULL) {
        return -1;
    }
    /* Удаляем \n в конце, если есть */
    size_t len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n') {
        buf[len - 1] = '\0';
    }
    return 0;
}

/* Подсчёт количества UTF-8 символов в строке */
static size_t utf8_strlen(const char *str)
{
    size_t len = 0;
    while (*str) {
        /* Если байт не является continuation byte (10xxxxxx), считаем символ */
        if ((*str & 0xC0) != 0x80) {
            len++;
        }
        str++;
    }
    return len;
}

/* Вывод строки с фиксированной шириной (в символах UTF-8) */
static void print_utf8_padded(const char *str, int width)
{
    size_t char_len = utf8_strlen(str);
    size_t i;
    printf("%s", str);
    /* Добавляем пробелы для выравнивания */
    for (i = char_len; i < (size_t)width; i++) {
        putchar(' ');
    }
}

/* ----------------------------------------- */
/* ГЛАВНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ПОДКЛЮЧЕНИЯ И ПАРАМЕТРОВ */
/* ----------------------------------------- */
exec SQL begin declare section;
    /* --- Данные для подключения (Ваши учетные данные) --- */
    char db_conn_info[128]; /* students@127.0.0.1:5432 */
    char user[50];        /* pmi-b2713 */
    char password[50];    /* l9C80!DaN */
    char stmt_set_path[] = "SET search_path TO pmib2713"; 
    char stmt_set_encoding[] = "SET client_encoding TO 'UTF8'"; 

    /* --- Параметры ввода --- */
    char param_n_post[7]; 
    char param_n_izd[7];  
    
    /* --- Результаты Задания 1 (Однострочный) --- */
    float sr_max_kol_res;
    short sr_max_kol_res_ind;

    /* --- Результаты Задания 2 (Многострочный) --- */
    char n_izd_2[7];
    char name_2[31];
    char town_2[21];
    float sr_kol_2;
    short n_izd_2_ind;
    short name_2_ind;
    short town_2_ind;
    short sr_kol_2_ind;

    /* --- Результаты Задания 3 (Многострочный) --- */
    char cvet_3[21];
    int to_cvet_3;
    int total_3;
    float percent_3;
    short cvet_3_ind;
    short to_cvet_3_ind;
    short total_3_ind;
    short percent_3_ind;

    /* --- Тексты динамических SQL-операторов (С ЯВНОЙ КВАЛИФИКАЦИЕЙ СХЕМЫ) --- */
    char stmt1_text[] = 
        "SELECT ROUND(AVG(b.max_kol), 2) AS sr_max_kol "
        "FROM ( "
            "SELECT MAX(kol) AS max_kol "
            "FROM pmib2713.spj "
            "GROUP BY n_izd "
        ") b";

    char stmt2_text[] = 
        "SELECT j.n_izd, j.name, j.town, b.sr_kol "
        "FROM ( "
            "SELECT n_izd, AVG(kol) AS sr_kol "
            "FROM pmib2713.spj "
            "WHERE n_post = ? "
            "GROUP BY n_izd "
        ") b "
        "JOIN pmib2713.j AS j ON b.n_izd = j.n_izd "
        "ORDER BY j.n_izd";

    char stmt3_text[] = 
        "SELECT a.cvet, a.to_cvet, b.total, ROUND(a.to_cvet * 100.0 / b.total, 2) AS percent "
        "FROM ( "
            "SELECT p.cvet, COUNT(*) AS to_cvet "
            "FROM pmib2713.spj "
            "JOIN pmib2713.p AS p ON pmib2713.spj.n_det = p.n_det "
            "WHERE pmib2713.spj.n_izd = ? "
            "GROUP BY p.cvet "
        ") a "
        "CROSS JOIN ( "
            "SELECT COUNT(*) AS total "
            "FROM pmib2713.spj "
            "WHERE n_izd = ? "
        ") b";
exec SQL end declare section;

/* ----------------------------------------- */
/* ПРОТОТИПЫ ФУНКЦИЙ */
/* ----------------------------------------- */
int ConnectDB(void);      /* Возвращает 0 при успехе, -1 при ошибке */
void DisconnectDB(void);
void PrintMenu(void);
int PrepareStatements(void); /* Возвращает 0 при успехе, -1 при ошибке */
void Task1(void);
void Task2(void);
void Task3(void);

/* ----------------------------------------- */
/* CONNECT DB */
/* ----------------------------------------- */
int ConnectDB(void)
{
    /* Установка данных подключения */
    strcpy(db_conn_info, "students@127.0.0.1:5432"); 
    strcpy(user, "pmi-b2713");
    strcpy(password, "l9C80!DaN");

    printf("Подключение к БД \"%s\"...\n", db_conn_info);
    
    /* 1. Динамический оператор CONNECT */
    exec SQL connect to :db_conn_info user :user using :password;
    
    if (sqlca.sqlcode < 0)
    {
        fprintf(stderr, "Ошибка подключения! код %d: %s\n",
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return -1;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
    
    /* 2. Динамическая установка схемы */
    printf("Подключение к схеме \"pmib2713\"...\n");
    exec SQL EXECUTE IMMEDIATE :stmt_set_path; 

    if (sqlca.sqlcode < 0)
    {
        fprintf(stderr, "Ошибка установки схемы! код %d: %s\n",
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL disconnect ALL;
        return -1;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
    
    /* 3. Динамическая установка кодировки клиента */
    printf("Установка кодировки клиента UTF8...\n");
    exec SQL EXECUTE IMMEDIATE :stmt_set_encoding; 

    if (sqlca.sqlcode < 0)
    {
        fprintf(stderr, "Предупреждение: Ошибка установки кодировки! код %d: %s\n",
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        /* Не фатальная ошибка, продолжаем работу */
    }
    else
    {
        printf("Успех! код %d\n", sqlca.sqlcode);
    }
    
    return 0;
}

/* ----------------------------------------- */
/* DISCONNECT DB */
/* ----------------------------------------- */
void DisconnectDB(void)
{
    printf("Отключение от БД...\n");
    exec SQL disconnect ALL;
    
    if (sqlca.sqlcode < 0)
    {
        fprintf(stderr, "Ошибка отключения! код %d: %s\n",
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Успех! код %d\n", sqlca.sqlcode);
}

/* ----------------------------------------- */
/* PREPARE STATEMENTS */
/* ----------------------------------------- */
int PrepareStatements(void)
{
    printf("\nПодготовка SQL-операторов...\n");
    
    exec SQL PREPARE stmt1 FROM :stmt1_text;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Ошибка подготовки stmt1: %s\n", sqlca.sqlerrm.sqlerrmc);
        return -1;
    }

    exec SQL PREPARE stmt2 FROM :stmt2_text;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Ошибка подготовки stmt2: %s\n", sqlca.sqlerrm.sqlerrmc);
        return -1;
    }
    
    exec SQL PREPARE stmt3 FROM :stmt3_text;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Ошибка подготовки stmt3: %s\n", sqlca.sqlerrm.sqlerrmc);
        return -1;
    }
    
    printf("Все операторы успешно подготовлены.\n");
    return 0;
}

/* ----------------------------------------- */
/* MENU */
/* ----------------------------------------- */
void PrintMenu(void)
{
    printf("1) Задание 1 (Среднее от Max объема)\n");
    printf("2) Задание 2 (Средний объем для Поставщика S*)\n");
    printf("3) Задание 3 (Процент поставок по Цвету для Изделия J*)\n");
    printf("0) Выход из программы\n");
}

/* ----------------------------------------- */
/* TASK 1 */
/* ----------------------------------------- */
void Task1(void)
{
    printf("\n--- Задание 1: Среднее от максимальных объемов поставок ---\n");
    
    exec SQL BEGIN WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 1: Ошибка начала транзакции! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция начата.\n");

    exec SQL EXECUTE stmt1 INTO :sr_max_kol_res :sr_max_kol_res_ind;

    if (sqlca.sqlcode < 0)
    {
        fprintf(stderr, "Задание 1: Ошибка выполнения! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL ROLLBACK WORK;
        return;
    }
    
    if (sqlca.sqlcode == 100)
    {
        printf("Задание 1: Данные не найдены.\n");
        exec SQL COMMIT WORK;
        return;
    }
    
    printf("Задание 1: Успех!\n");
    if (sr_max_kol_res_ind < 0)
    {
        printf("   Среднее максимальных объемов поставок для каждого изделия: NULL\n");
    }
    else
    {
        printf("   Среднее максимальных объемов поставок для каждого изделия: %.2f\n", sr_max_kol_res);
    }
    
    exec SQL COMMIT WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 1: Ошибка COMMIT! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция завершена (COMMIT).\n");
}

/* ----------------------------------------- */
/* TASK 2 */
/* ----------------------------------------- */
void Task2(void)
{
    int rowcount = 0;
    
    printf("\n--- Задание 2: Средний объем поставок для поставщика S* ---\n");

    /* Ввод параметра */
    printf("Введите номер поставщика S* (например, S2): ");
    if (read_line(param_n_post, sizeof(param_n_post)) != 0 || param_n_post[0] == '\0') {
        fprintf(stderr, "Ошибка ввода.\n");
        return;
    }
    printf("Используется поставщик: %s\n", param_n_post);

    exec SQL BEGIN WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 2: Ошибка начала транзакции! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция начата.\n");
    
    exec SQL DECLARE cursor2 CURSOR FOR stmt2;
    /* DECLARE в ECPG - декларативный, ошибки при повторном объявлении игнорируем */

    exec SQL OPEN cursor2 USING :param_n_post;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 2: Ошибка открытия курсора! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL ROLLBACK WORK;
        return;
    }
    
    printf("\n+--------+------------------+---------------+-----------+\n");
    printf("| ");
    print_utf8_padded("ИЗД №", 6);
    printf(" | ");
    print_utf8_padded("НАЗВАНИЕ ИЗДЕЛИЯ", 16);
    printf(" | ");
    print_utf8_padded("ГОРОД ИЗДЕЛИЯ", 13);
    printf(" | ");
    print_utf8_padded("СР. ОБЪЕМ", 9);
    printf(" |\n");
    printf("+--------+------------------+---------------+-----------+\n");

    while (1)
    {
        exec SQL FETCH cursor2 INTO 
            :n_izd_2 :n_izd_2_ind, 
            :name_2 :name_2_ind, 
            :town_2 :town_2_ind, 
            :sr_kol_2 :sr_kol_2_ind;

        if (sqlca.sqlcode == 100) break;
        if (sqlca.sqlcode < 0)
        {
            fprintf(stderr, "Задание 2: Ошибка получения данных! код %d: %s\n", 
                    sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
            exec SQL CLOSE cursor2;
            exec SQL ROLLBACK WORK;
            return;
        }
        
        /* Убираем trailing пробелы из CHAR полей */
        if (n_izd_2_ind >= 0) rtrim(n_izd_2);
        if (name_2_ind >= 0) rtrim(name_2);
        if (town_2_ind >= 0) rtrim(town_2);
        
        printf("| ");
        print_utf8_padded((n_izd_2_ind < 0) ? "NULL" : n_izd_2, 6);
        printf(" | ");
        print_utf8_padded((name_2_ind < 0) ? "NULL" : name_2, 16);
        printf(" | ");
        print_utf8_padded((town_2_ind < 0) ? "NULL" : town_2, 13);
        printf(" | %9.2f |\n", (sr_kol_2_ind < 0) ? 0.0 : sr_kol_2);
        rowcount++;
    }
    printf("+--------+------------------+---------------+-----------+\n");
    
    if (rowcount == 0) {
        printf("Данных для поставщика %s не найдено.\n", param_n_post);
    } else {
        printf("Задание 2: Успех! Строк: %d\n", rowcount);
    }

    exec SQL CLOSE cursor2;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 2: Ошибка закрытия курсора! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL ROLLBACK WORK;
        return;
    }

    exec SQL COMMIT WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 2: Ошибка COMMIT! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция завершена (COMMIT).\n");
}

/* ----------------------------------------- */
/* TASK 3 */
/* ----------------------------------------- */
void Task3(void)
{
    int rowcount = 0;
    
    printf("\n--- Задание 3: Процент поставок по цвету для изделия J* ---\n");

    /* Ввод параметра */
    printf("Введите номер изделия J* (например, J4): ");
    if (read_line(param_n_izd, sizeof(param_n_izd)) != 0 || param_n_izd[0] == '\0') {
        fprintf(stderr, "Ошибка ввода.\n");
        return;
    }
    printf("Используется изделие: %s\n", param_n_izd);
    
    exec SQL BEGIN WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 3: Ошибка начала транзакции! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция начата.\n");
    
    exec SQL DECLARE cursor3 CURSOR FOR stmt3;
    /* DECLARE в ECPG - декларативный, ошибки при повторном объявлении игнорируем */

    exec SQL OPEN cursor3 USING :param_n_izd, :param_n_izd;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 3: Ошибка открытия курсора! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL ROLLBACK WORK;
        return;
    }
    
    printf("\n+-------------+-------------+----------------+-------------+\n");
    printf("| ");
    print_utf8_padded("ЦВЕТ ДЕТАЛИ", 11);
    printf(" | ");
    print_utf8_padded("ЧИСЛО ПОСТ.", 11);
    printf(" | ");
    print_utf8_padded("ВСЕГО ПОСТ. J*", 14);
    printf(" | ");
    print_utf8_padded("ПРОЦЕНТ (%)", 11);
    printf(" |\n");
    printf("+-------------+-------------+----------------+-------------+\n");

    while (1)
    {
        exec SQL FETCH cursor3 INTO 
            :cvet_3 :cvet_3_ind, 
            :to_cvet_3 :to_cvet_3_ind, 
            :total_3 :total_3_ind, 
            :percent_3 :percent_3_ind;

        if (sqlca.sqlcode == 100) break;
        if (sqlca.sqlcode < 0)
        {
            fprintf(stderr, "Задание 3: Ошибка получения данных! код %d: %s\n", 
                    sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
            exec SQL CLOSE cursor3;
            exec SQL ROLLBACK WORK;
            return;
        }
        
        /* Убираем trailing пробелы из CHAR полей */
        if (cvet_3_ind >= 0) rtrim(cvet_3);
        
        printf("| ");
        print_utf8_padded((cvet_3_ind < 0) ? "NULL" : cvet_3, 11);
        printf(" | %11d | %14d | %11.2f |\n", 
               (to_cvet_3_ind < 0) ? 0 : to_cvet_3, 
               (total_3_ind < 0) ? 0 : total_3, 
               (percent_3_ind < 0) ? 0.0 : percent_3);
        rowcount++;
    }
    printf("+-------------+-------------+----------------+-------------+\n");

    if (rowcount == 0) {
        printf("Данных по цвету для изделия %s не найдено.\n", param_n_izd);
    } else {
        printf("Задание 3: Успех! Строк: %d\n", rowcount);
    }
    
    exec SQL CLOSE cursor3;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 3: Ошибка закрытия курсора! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        exec SQL ROLLBACK WORK;
        return;
    }
    
    exec SQL COMMIT WORK;
    if (sqlca.sqlcode < 0) {
        fprintf(stderr, "Задание 3: Ошибка COMMIT! код %d: %s\n", 
                sqlca.sqlcode, sqlca.sqlerrm.sqlerrmc);
        return;
    }
    printf("Транзакция завершена (COMMIT).\n");
}

/* ----------------------------------------- */
/* MAIN */
/* ----------------------------------------- */
int main(void)
{
    int number;
    
    if (ConnectDB() != 0) {
        fprintf(stderr, "Программа завершена из-за ошибки подключения.\n");
        return 1;
    }
    
    /* Очистка незавершенных транзакций (для устранения ошибки -603) */
    exec SQL ROLLBACK WORK;

    if (PrepareStatements() != 0) {
        fprintf(stderr, "Программа завершена из-за ошибки подготовки SQL.\n");
        DisconnectDB();
        return 1;
    }

    while (true)
    {
        printf("\nЧто выполнить?\n");
        PrintMenu();
        printf("Выберите номер: ");
        
        if (scanf("%d", &number) != 1)
        {
            clear_input_buffer();
            printf("Некорректный ввод, попробуйте снова.\n");
            continue;
        }
        clear_input_buffer(); /* Очищаем остаток строки после числа */
        
        switch (number)
        {
            case 1: Task1(); break;
            case 2: Task2(); break;
            case 3: Task3(); break;
            case 0:
                DisconnectDB();
                return 0;
            default:
                printf("Выберите из доступных номеров!\n");
                break;
        }
    }
}