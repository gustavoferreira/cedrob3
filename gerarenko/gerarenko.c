//g++-11 -std=c++17 gerarenko.c -lboost_system -lpthread
#ifdef __cplusplus
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <ctime>
#include <unistd.h>
using namespace std;
#else
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#endif

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <ctype.h>
#include <dirent.h>

#define MAX_LINE_LENGTH 1024
#define MAX_RENKO_SIZES 10
#define MAX_SYMBOLS 3

// Adicionar novas constantes
#define MARKET_OPEN_HOUR 0
#define MARKET_OPEN_MIN 0  // Market opens at 09:00
#define MARKET_CLOSE_HOUR 24
#define SLEEP_INTERVAL_MS 100

// Structure to hold Renko configuration
typedef struct {
    char asset[16];
    double factor;
    int num_sizes;
    int sizes[MAX_RENKO_SIZES];
} RenkoConfig;

// Add new structure to hold trade data
typedef struct {
    char asset[16];
    char operation;           // A, D, R
    char time[16];           // HHMMSSXXX format
    double price;
    char buyer[16];          // Buying broker ID
    char seller[16];         // Selling broker ID
    int quantity;
    int trade_id;
    int direct;              // 0=Not direct, 1=Direct, 2=RLP
    char aggressor;          // I=Undefined, A=Buyer, V=Seller
    int request_id;          // Only for snapshot messages
} TradeData;

char* extract_base_symbol(const char* full_symbol) {
    static char base_symbol[16];

    // Check if symbol starts with W (WIN/WDO cases)
    if (full_symbol[0] == 'W') {
        // Check second letter to determine if WIN or WDO
        if (full_symbol[1] == 'I') {
            strcpy(base_symbol, "WIN");
        }
        else if (full_symbol[1] == 'D') {
            strcpy(base_symbol, "WDO");
        }
        else {
            strcpy(base_symbol, "W"); // fallback
        }
    }
    // Check for DI case
    else if (strncmp(full_symbol, "DI", 2) == 0) {
        strcpy(base_symbol, "DI");
    }
    else {
        // Copy until first non-letter character for other cases
        int i;
        for (i = 0; full_symbol[i] && isalpha(full_symbol[i]); i++) {
            base_symbol[i] = full_symbol[i];
        }
        base_symbol[i] = '\0';
    }

    return base_symbol;
}

// Add function to parse trade message
TradeData parse_trade_message(const char* line) {
    TradeData trade = { 0 };
    char* token = NULL, * next_token = NULL;
    char* line_copy;
    int field = 0;

    // Skip "V:" prefix and get asset
#ifdef _WIN32
    line_copy = _strdup(line + 2);  // Use _strdup instead of strdup
    token = strtok_s(line_copy, ":", &next_token);  // Use strtok_s instead of strtok_r
#else
    line_copy = strdup(line + 2);  // Use strdup for Linux
    token = strtok_r(line_copy, ":", &next_token);  // Use strtok_r for Linux
#endif
    if (token) {
        // Extract base symbol from full contract code
        char* base_symbol = extract_base_symbol(token);
        strncpy(trade.asset, base_symbol, sizeof(trade.asset) - 1);
    }

    // Parse remaining fields
#ifdef _WIN32
    while ((token = strtok_s(NULL, ":", &next_token)) != NULL) {
#else
    while ((token = strtok_r(NULL, ":", &next_token)) != NULL) {
#endif
        field++;
        switch (field) {
        case 1: // Operation
            trade.operation = token[0];
            break;
        case 2: // Trade time
            strncpy(trade.time, token, sizeof(trade.time) - 1);
            break;
        case 3: // PriceW
            trade.price = atof(token);
            break;
        case 4: // Buyer broker
            strncpy(trade.buyer, token, sizeof(trade.buyer) - 1);
            break;
        case 5: // Seller broker
            strncpy(trade.seller, token, sizeof(trade.seller) - 1);
            break;
        case 6: // Quantity
            trade.quantity = atoi(token);
            break;
        case 7: // Trade ID
            trade.trade_id = atoi(token);
            break;
        case 8: // Direct/Request ID
            if (trade.operation == 'A')
                trade.direct = atoi(token);
            else
                trade.request_id = atoi(token);
            break;
        case 9: // Aggressor (only for add operations)
            if (trade.operation == 'A')
                trade.aggressor = token[0];
            break;
        }
    }
    free(line_copy);
    return trade;
}

// Função para verificar horário de mercado
int is_market_hours() {
    time_t now = time(NULL);
    struct tm* tm_now = localtime(&now);
    return (tm_now->tm_hour >= MARKET_OPEN_HOUR &&
        tm_now->tm_hour < MARKET_CLOSE_HOUR);
}

// Function to process the raw data file
// Inside the process_raw_data function, add tracking for the last processed trade ID
void process_raw_data(const char* raw_file, const RenkoConfig* configs, int num_configs, const char* day, int is_historical) {
    FILE* input_file = fopen(raw_file, "r");
    if (!input_file) {
        printf("Error opening file: %s\n", raw_file);
        return;
    }

    // Open booking data file
    char booking_file[256];
    sprintf(booking_file, "/home/grao/dados/renko_files/%s_booking_data.txt", day);
    FILE* booking_output = fopen(booking_file, "w");
    if (!booking_output) {
        printf("Error creating booking file: %s\n", booking_file);
        fclose(input_file);
        return;
    }

    // Prepare Renko output files for each symbol
    FILE* renko_files[MAX_SYMBOLS][MAX_RENKO_SIZES];
    double current_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
    double high_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
    double low_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
    double brick_sizes[MAX_SYMBOLS][MAX_RENKO_SIZES];

    for (int s = 0; s < num_configs; s++) {
        for (int i = 0; i < configs[s].num_sizes; i++) {
            char renko_file[256];
	    sprintf(renko_file, "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", day, configs[s].asset, configs[s].sizes[i]);
            renko_files[s][i] = fopen(renko_file, "w");
            if (!renko_files[s][i]) {
                printf("Error creating Renko file: %s\n", renko_file);
                fclose(input_file);
                fclose(booking_output);
                return;
            }
            // Write header
            fprintf(renko_files[s][i], "data,time,open,high,low,close\n");
            current_prices[s][i] = 0.0;
            high_prices[s][i] = 0.0;
            low_prices[s][i] = 9999999.0;
            brick_sizes[s][i] = configs[s].sizes[i] * configs[s].factor;
        }
    }

    char line[MAX_LINE_LENGTH];
    char current_time_str[20];
    long last_position = 0;
    int eof_count = 0;
    
    // Add variables to track the last processed trade IDs for each symbol and brick size
    int last_trade_ids[MAX_SYMBOLS][MAX_RENKO_SIZES];
    
    // Initialize all last trade IDs to -1 (not processed yet)
    for (int s = 0; s < num_configs; s++) {
        for (int i = 0; i < configs[s].num_sizes; i++) {
            last_trade_ids[s][i] = -1;
        }
    }

    while (1) {
        // Verificar horário de mercado apenas no modo tempo real
        if (!is_historical && !is_market_hours()) {
            printf("Outside market hours (09:00-19:00). Exiting...\n");
            break;
        }

        // Salvar posição atual no arquivo
        last_position = ftell(input_file);

        if (fgets(line, sizeof(line), input_file)) {
            eof_count = 0; // Resetar contador de EOF quando há dados

            // Process booking data
            if (strncmp(line, "B:", 2) == 0) {
                fprintf(booking_output, "%s", line);
                continue;
            }

            // Process trade data (V:)
            if (strncmp(line, "V:", 2) == 0) {
                TradeData trade = parse_trade_message(line);

                // Skip if not an add operation
                if (trade.operation != 'A') continue;

                // Parse time components
                int hour, min, sec, msec;
                if (sscanf(trade.time, "%2d%2d%2d%3d", &hour, &min, &sec, &msec) != 4) {
                    continue;
                }
                int data_int = atoi(day);
                int ano = data_int / 10000;
                int mes = (data_int / 100) % 100;
                int dia = data_int % 100;

                // Preenche struct tm
                struct tm tm_day = { 0 };
                tm_day.tm_year = ano - 1900; // anos desde 1900
                tm_day.tm_mon = mes - 1;     // meses de 0 a 11
                tm_day.tm_mday = dia;
                tm_day.tm_hour = hour;
                tm_day.tm_min = min;
                tm_day.tm_sec = sec;

                // Converte para time_t
                time_t timestamp = mktime(&tm_day);
                timestamp += 1;//3 * 3600;
                // Converte o timestamp ajustado de volta para struct tm
                struct tm *tm_adj = localtime(&timestamp);                
                
                // Debug output to verify timestamp
                //char debug_time[30];
                //strftime(debug_time, sizeof(debug_time), "%Y%m%d %H:%M:%S", localtime(&timestamp));
                //printf("Generated timestamp: %s.%03d (%ld.%03d)\n", debug_time, msec, timestamp, msec);

                // Find the configuration for the current asset
                int config_index = -1;
                for (int s = 0; s < num_configs; s++) {
                    if (strcmp(configs[s].asset, trade.asset) == 0) {
                        config_index = s;
                        break;
                    }
                }
                if (config_index == -1) {
                    continue; // Skip unknown assets
                }

                // Process each Renko size for the current asset
                for (int i = 0; i < configs[config_index].num_sizes; i++) {
                    // Skip if we've already processed this trade ID for this symbol and brick size
                    if (trade.trade_id <= last_trade_ids[config_index][i] && last_trade_ids[config_index][i] != -1) {
                        continue;
                    }
                    
                    double brick_size = brick_sizes[config_index][i];
                    
                    // Initialize price if this is the first trade
                    if (current_prices[config_index][i] == 0.0) {
                        current_prices[config_index][i] = trade.price;
                        char time_str[20];
                        strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                        fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                            time_str, timestamp, msec,
                            trade.price, trade.price, trade.price, trade.price);
                        fflush(renko_files[config_index][i]);
                        strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                    }
                    
                    // Track the current trend direction (1 for up, -1 for down)
                    static int trend_direction[MAX_SYMBOLS][MAX_RENKO_SIZES] = {0};
                    if (trend_direction[config_index][i] == 0) {
                        // Initialize direction on first run
                        trend_direction[config_index][i] = 1;  // Start with up direction
                    }
                    
                    double current_price = current_prices[config_index][i];
                    double price_diff;
                    if (trade.price > high_prices[config_index][i]) {
                        high_prices[config_index][i] = trade.price;
                    }
                    if (trade.price < low_prices[config_index][i]) {
                        low_prices[config_index][i] = trade.price;
                    }
                    double high_price = high_prices[config_index][i];
                    double low_price = low_prices[config_index][i];
                    // Calculate how many bricks to generate based on direction
                    if (trend_direction[config_index][i] > 0) {
                        // Current trend is up
                        if (trade.price >= current_price + brick_size) {
                            // Continue uptrend - generate up bricks
                            int num_bricks = (int)((trade.price - current_price) / brick_size);
                            
                            for (int j = 0; j < num_bricks; j++) {
                                double brick_open = current_price;
                                double brick_close = brick_open + brick_size;
                                
                                char time_str[20];
                                strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                
                                // Write Renko brick with formatted date and time
                                fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                    time_str,
                                    timestamp, msec,
                                    brick_open,
                                    brick_close,  // High is close for up brick
                                    brick_open,   // Low is open for up brick
                                    brick_close);
                                fflush(renko_files[config_index][i]);
                                strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                current_price = brick_close;
                                high_prices[config_index][i] = brick_close;
                                low_prices[config_index][i] = brick_close;
                            }
                        } 
                        else if (trade.price < current_price - (brick_size * 2)) {
                            // Reversal from up to down - need at least 2 bricks to change direction
                            trend_direction[config_index][i] = -1;
                            
                            // Calculate how many down bricks after reversal
                            int num_bricks = (int)((current_price - trade.price) / brick_size);
                            
                            // First brick for reversal
                            double brick_open = current_price;
                            double brick_close = brick_open - brick_size;
                            
                            char time_str[20];
                            strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                            
                            // Write reversal brick

                            fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str,
                                timestamp, msec,
                                brick_open,
                                high_price,   // High is open for down brick
                                low_price,  // Low is close for down brick
                                brick_close);
                            fflush(renko_files[config_index][i]);
                            current_price = brick_close;
                            high_prices[config_index][i] = brick_close;
                            low_prices[config_index][i] = brick_close;
                            strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                            
                            // Additional bricks after reversal
                            for (int j = 1; j < num_bricks; j++) {
                                brick_open = current_price;
                                brick_close = brick_open - brick_size;
                                fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                    time_str,
                                    timestamp, msec,
                                    brick_open,
                                    high_price,   // High is open for down brick
                                    low_price,  // Low is close for down brick
                                    brick_close);
                                fflush(renko_files[config_index][i]);
                                strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                current_price = brick_close;
                                high_prices[config_index][i] = brick_close;
                                low_prices[config_index][i] = brick_close;
                            }
                        }
                    } 
                    else {
                        // Current trend is down
                        if (trade.price < current_price - brick_size) {
                            // Continue downtrend - generate down bricks
                            int num_bricks = (int)((current_price - trade.price) / brick_size);
                            
                            for (int j = 0; j < num_bricks; j++) {
                                double brick_open = current_price;
                                double brick_close = brick_open - brick_size;
                                
                                char time_str[20];
                                strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                // Write Renko brick with formatted date and time
                                fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                    time_str,
                                    timestamp, msec,
                                    brick_open,
                                    high_price,   // High is open for down brick
                                    low_price,  // Low is close for down brick
                                    brick_close);
                                fflush(renko_files[config_index][i]);
                                strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                current_price = brick_close;
                                high_prices[config_index][i] = brick_close;
                                low_prices[config_index][i] = brick_close;
                            }
                        } 
                        else if (trade.price > current_price + (brick_size * 2)) {
                            // Reversal from down to up - need at least 2 bricks to change direction
                            trend_direction[config_index][i] = 1;
                            
                            // Calculate how many up bricks after reversal
                            int num_bricks = (int)((trade.price - current_price) / brick_size);
                            
                            // First brick for reversal
                            double brick_open = current_price;
                            double brick_close = brick_open + brick_size;
                            
                            char time_str[20];
                            strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                            
                            // Write reversal brick
                            fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str,
                                timestamp, msec,
                                brick_open,
                                high_price,  // High is close for up brick
                                low_price,   // Low is open for up brick
                                brick_close);
                            fflush(renko_files[config_index][i]);
                            strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                            current_price = brick_close;
                            high_prices[config_index][i] = brick_close;
                            low_prices[config_index][i] = brick_close;
                            
                            // Additional bricks after reversal
                            for (int j = 1; j < num_bricks; j++) {
                                brick_open = current_price;
                                brick_close = brick_open + brick_size;
                                fprintf(renko_files[config_index][i], "%s,%ld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                    time_str,
                                    timestamp, msec,
                                    brick_open,
                                    high_price,  // High is close for up brick
                                    low_price,   // Low is open for up brick
                                    brick_close);
                                fflush(renko_files[config_index][i]);
                                strftime(current_time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", tm_adj);
                                current_price = brick_close;
                                high_prices[config_index][i] = brick_close;
                                low_prices[config_index][i] = brick_close;
                            }
                        }
                    }
                    
                    // Update the current price for this symbol and brick size
                    current_prices[config_index][i] = current_price;

                }
            }
        }
        else {
            // Chegou ao fim do arquivo
            if (is_historical) {
                // No modo histórico, simplesmente terminar quando chegar ao final
                printf("Fim do arquivo histórico alcançado.\n");
                break;
            }
            
            // Modo tempo real: tentar reabrir arquivo e aguardar novos dados
            eof_count++;

            if (eof_count >= 10) { // Após algumas tentativas
                // Reabrir o arquivo para pegar novos dados
                fclose(input_file);
                input_file = fopen(raw_file, "r");
                if (!input_file) {
                    printf("Error reopening file\n");
                    break;
                }

                // Voltar para última posição
                fseek(input_file, last_position, SEEK_SET);
                eof_count = 0;
            }

            // Aguardar antes de tentar ler novamente
#ifdef _WIN32
            Sleep(SLEEP_INTERVAL_MS);
#else
            usleep(SLEEP_INTERVAL_MS * 1000);
#endif
        }
    }

    // Close all files
    fclose(input_file);
    fclose(booking_output);
    for (int s = 0; s < num_configs; s++) {
        for (int i = 0; i < configs[s].num_sizes; i++) {
            fclose(renko_files[s][i]);
        }
    }
}

// Add a new structure to track the last processed trade for each symbol/brick size combination
typedef struct {
    char asset[16];
    int brick_size;
    int last_processed_trade_id;
    time_t last_update_time;
} ProcessedTradeInfo;

// Add a global array to store the last processed trade info
ProcessedTradeInfo processed_trades[MAX_SYMBOLS * MAX_RENKO_SIZES];
int num_processed_trades = 0;

// Function to find or create a processed trade info entry
ProcessedTradeInfo* find_or_create_processed_trade_info(const char* asset, int brick_size) {
    // First, try to find an existing entry
    for (int i = 0; i < num_processed_trades; i++) {
        if (strcmp(processed_trades[i].asset, asset) == 0 && 
            processed_trades[i].brick_size == brick_size) {
            return &processed_trades[i];
        }
    }
    
    // If not found and we have space, create a new entry
    if (num_processed_trades < MAX_SYMBOLS * MAX_RENKO_SIZES) {
        strcpy(processed_trades[num_processed_trades].asset, asset);
        processed_trades[num_processed_trades].brick_size = brick_size;
        processed_trades[num_processed_trades].last_processed_trade_id = 0;
        processed_trades[num_processed_trades].last_update_time = time(NULL);
        return &processed_trades[num_processed_trades++];
    }
    
    // If we're out of space, return NULL
    return NULL;
}


// Add this at the top of the file, after the includes
#include <stdlib.h>

// Add this function to set Brazil timezone
void set_brazil_timezone() {
    // Set timezone to Brazil (UTC-3)
    #ifdef _WIN32
    _putenv_s("TZ", "BRT");  // Format for Windows
    #else
    setenv("TZ", "America/Sao_Paulo", 1);
    #endif
    tzset();
    
    // Debug output to verify timezone setting
    time_t test_time = time(NULL);
    struct tm* tm_test = localtime(&test_time);
    printf("Current time: %04d-%02d-%02d %02d:%02d:%02d\n", 
           tm_test->tm_year + 1900, tm_test->tm_mon + 1, tm_test->tm_mday,
           tm_test->tm_hour, tm_test->tm_min, tm_test->tm_sec);
}

// Modify the main function to set the timezone at the beginning
// Função para processar arquivos históricos
void process_historical_files(const RenkoConfig* configs, int num_configs) {
    printf("Processando arquivos históricos...\n");
    
    DIR *dir;
    struct dirent *entry;
    const char *directory_path = "/home/grao/dados/cedro_files";
    
    dir = opendir(directory_path);
    if (dir == NULL) {
        printf("Erro: Não foi possível abrir o diretório %s\n", directory_path);
        return;
    }
    
    int files_processed = 0;
    
    // Ler todos os arquivos do diretório
    while ((entry = readdir(dir)) != NULL) {
        // Verificar se é um arquivo regular e se tem extensão .txt
        if (entry->d_type == DT_REG && strstr(entry->d_name, ".txt") != NULL) {
            char full_path[512];
            sprintf(full_path, "%s/%s", directory_path, entry->d_name);
            
            // Extrair a data do nome do arquivo (assumindo formato YYYYMMDD_raw_data.txt)
            char day[9] = {0};
            if (strlen(entry->d_name) >= 8) {
                strncpy(day, entry->d_name, 8);
                day[8] = '\0';
            }
            
            // Verificar se os arquivos Renko já existem para esta data
            int already_processed = 1;
            for (int s = 0; s < num_configs && already_processed; s++) {
                for (int i = 0; i < configs[s].num_sizes && already_processed; i++) {
                    char renko_file_check[256];
                    sprintf(renko_file_check, "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                           day, configs[s].asset, configs[s].sizes[i]);
                    FILE* check_file = fopen(renko_file_check, "r");
                    if (!check_file) {
                        already_processed = 0; // Pelo menos um arquivo não existe
                    } else {
                        fclose(check_file);
                    }
                }
            }
            
            if (already_processed) {
                printf("Arquivos Renko já existem para %s, pulando...\n", day);
            } else {
                printf("Processando arquivo histórico: %s\n", full_path);
                process_raw_data(full_path, configs, num_configs, day, 1); // 1 = modo histórico
                files_processed++;
            }
        }
    }
    
    closedir(dir);
    
    if (files_processed == 0) {
        printf("Nenhum arquivo .txt encontrado no diretório %s\n", directory_path);
    } else {
        printf("Processamento histórico concluído. %d arquivos processados.\n", files_processed);
    }
}

int main(int argc, char* argv[]) {
    // Set Brazil timezone
    set_brazil_timezone();
    
    // Configure Renko settings for multiple symbols
    RenkoConfig configs[MAX_SYMBOLS] = {
        {.asset = "DI", .factor = 0.1, .num_sizes = 2, .sizes = {3, 5} },
        {.asset = "WDO", .factor = 0.5, .num_sizes = 3, .sizes = {5, 7, 10} },
        {.asset = "WIN", .factor = 5.0, .num_sizes = 3, .sizes = {10, 20, 30} }
    };
    
    // Verificar argumentos de linha de comando
    int realtime_mode = 1;
    int historical_mode = 0;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "realtime=True") == 0 || strcmp(argv[i], "realtime=true") == 0) {
            realtime_mode = 1;
        }
        else if (strcmp(argv[i], "historico=True") == 0 || strcmp(argv[i], "historico=true") == 0) {
            historical_mode = 1;
        }
    }
    
    // Se nenhum modo foi especificado, usar modo realtime por padrão
    if (!realtime_mode && !historical_mode) {
        printf("Nenhum modo especificado. Usando modo realtime por padrão.\n");
        printf("Use: %s realtime=True ou %s historico=True\n", argv[0], argv[0]);
        realtime_mode = 1;
    }
    
    if (historical_mode) {
        process_historical_files(configs, MAX_SYMBOLS);
        return 0;
    }

    // Get current date
    time_t now = time(NULL);
    struct tm* tm_now = localtime(&now);
    char day[9];
    strftime(day, sizeof(day), "%Y%m%d", tm_now);
    //const char* day= "20251118";

    // Process raw data file
    char raw_file[256];
    sprintf(raw_file, "/home/grao/dados/cedro_files/%s_raw_data.txt", day);

    printf("Starting continuous processing for date: %s\n", day);
    printf("Will run from 09:00 to 19:00\n");
    
    // Wait until 09:00 if before market open
    while (1) {
        time_t current_time = time(NULL);
        struct tm* tm_current = localtime(&current_time);
        
        if (tm_current->tm_hour > MARKET_OPEN_HOUR || 
            (tm_current->tm_hour == MARKET_OPEN_HOUR && tm_current->tm_min >= MARKET_OPEN_MIN)) {
            break;  // It's after market open time
        }
        
        printf("Waiting for market open (09:00). Current time: %02d:%02d\n", 
               tm_current->tm_hour, tm_current->tm_min);
        
        // Sleep for 10 seconds before checking again
#ifdef _WIN32
        Sleep(10000);
#else
        sleep(10);
#endif
    }

    // Start processing data
    while (is_market_hours()) {
        process_raw_data(raw_file, configs, MAX_SYMBOLS, day, 0); // 0 = modo tempo real

        // Aguardar um pouco antes de tentar novamente
#ifdef _WIN32
        Sleep(1000);
#else
        sleep(1);
#endif
    }

    printf("Market closed. Processing completed.\n");
    return 0;
}

