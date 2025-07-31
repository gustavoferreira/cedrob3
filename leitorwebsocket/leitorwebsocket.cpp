//g++-11 -std=c++17 leitorwebsocket.cpp -lboost_system -lpthread
#include <iostream>
#include <boost/asio.hpp>
#include <fstream>
#include <ctime>
#include <experimental/filesystem>
#include <filesystem>
#include <chrono>
#include <thread>
#include <boost/algorithm/string.hpp>
#include <cmath>
#include <iomanip>

//#using boost::asio::ip::tcp;
namespace fs = std::filesystem;

#if BOOST_VERSION >= 106600
    namespace asio = boost::asio;
    using io_context_type = asio::io_context;
#else
    namespace asio = boost::asio;
    using io_context_type = asio::io_service;
#endif

// Constants from gera_renko.c
#define MAX_LINE_LENGTH 1024
#define MAX_RENKO_SIZES 10
#define MAX_SYMBOLS 3
#define MARKET_OPEN_HOUR 9
#define MARKET_OPEN_MIN 0
#define MARKET_CLOSE_HOUR 24
#define SLEEP_INTERVAL_MS 100

#ifndef _WIN32
#define _strdup strdup
#endif

// Structure to hold Renko configuration
typedef struct {
    char asset[16];
    double factor;
    int num_sizes;
    int sizes[MAX_RENKO_SIZES];
} RenkoConfig;

// Structure to hold trade data
typedef struct {
    char asset[16];
    char operation;
    char time[16];
    double price;
    char buyer[16];
    char seller[16];
    int quantity;
    int trade_id;
    int direct;
    char aggressor;
    int request_id;
} TradeData;

// Structure to track high/low values during brick formation
typedef struct {
    double brick_high;
    double brick_low;
} RenkoTrackingInfo;

// Global variables for Renko processing
FILE* renko_files[MAX_SYMBOLS][MAX_RENKO_SIZES];
double current_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
double high_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
double low_prices[MAX_SYMBOLS][MAX_RENKO_SIZES];
double brick_sizes[MAX_SYMBOLS][MAX_RENKO_SIZES];
int trend_direction[MAX_SYMBOLS][MAX_RENKO_SIZES] = {0};
int last_trade_ids[MAX_SYMBOLS][MAX_RENKO_SIZES];
RenkoTrackingInfo renko_tracking[MAX_SYMBOLS][MAX_RENKO_SIZES];
char current_time_str[20] = {0};

std::string get_current_date() {
    std::time_t t = std::time(nullptr);
    
    // Ajustar para horário brasileiro (UTC-3)
    t -= 3 * 3600; // 3 horas em segundos
    
    std::tm local_tm;
    gmtime_r(&t, &local_tm); // Usar gmtime_r pois já ajustamos o offset
    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d", &local_tm);
    return std::string(buffer);
}

bool is_time_to_stop() {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    
    // Ajustar para horário brasileiro (UTC-3)
    // Subtrair 3 horas do horário UTC para obter horário local brasileiro
    now -= 3 * 3600; // 3 horas em segundos
    
    std::tm local_tm;
    gmtime_r(&now, &local_tm); // Usar gmtime_r pois já ajustamos o offset
    
    return (local_tm.tm_hour >= 19 or local_tm.tm_hour < 9);
}

void log_response(const std::string& response, const std::string& symbol) {
    std::string date = get_current_date();
    std::string type = (response.rfind("V:", 0) == 0) ? "quote" : "booking";
    std::string filename = "/home/grao/dados/cedro_files/" + date + "_" + symbol + "_" + type + ".txt";

    try {
        // Create directories with error checking
        std::error_code ec;
        fs::create_directories("/home/grao/dados/cedro_files/", ec);
        if (ec) {
            std::cerr << "Erro ao criar diretório: " << ec.message() << std::endl;
            return;
        }
        
        std::ofstream log_file(filename, std::ios::app);
        if (!log_file.is_open()) {
            std::cerr << "Erro ao abrir arquivo: " << filename << std::endl;
            return;
        }
        
        log_file << response << std::endl;
        
        // Check if write was successful
        if (log_file.fail()) {
            std::cerr << "Erro ao escrever no arquivo: " << filename << std::endl;
            return;
        }
        
        log_file.close();
        
        // Verify file was closed properly
        if (log_file.fail()) {
            std::cerr << "Erro ao fechar arquivo: " << filename << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Exceção na função log_response: " << e.what() << std::endl;
    }
}


// Add this function to determine the correct WIN contract code
std::string get_win_contract() {
    std::time_t t = std::time(nullptr);
    
    // Ajustar para horário brasileiro (UTC-3)
    t -= 3 * 3600; // 3 horas em segundos
    
    std::tm local_tm;
    gmtime_r(&t, &local_tm); // Usar gmtime_r pois já ajustamos o offset

    int year = local_tm.tm_year % 100;  // Get last two digits of year
    int month = local_tm.tm_mon + 1;    // tm_mon is 0-based
    int day = local_tm.tm_mday;

    // WIN contracts use J(Apr), M(Jun), Q(Aug), V(Oct), Z(Dec), G(Feb)
    char month_codes[] = { 'G', 'G', 'J', 'J', 'M', 'M', 'Q', 'Q', 'V', 'V', 'Z', 'Z' };

    // Determine if we need to roll to next contract
    // For WIN, check if we're within 5 days of expiration (approx. 15th of even months)
    bool need_to_roll = false;

    if (month % 2 == 0) {  // Even month
        // If we're past the 10th day of the month, consider rolling to next contract
        if (day > 10) {
            need_to_roll = true;
        }
    }

    // If we need to roll, move to next contract
    if (need_to_roll) {
        month += 2;
        if (month > 12) {
            month -= 12;
            year++;
        }
    }

    // Get the correct month code based on contract month
    char month_code;
    switch(month) {
        case 2: month_code = 'G'; break;  // February
        case 4: month_code = 'J'; break;  // April
        case 6: month_code = 'M'; break;  // June
        case 8: month_code = 'Q'; break;  // August
        case 10: month_code = 'V'; break; // October
        case 12: month_code = 'Z'; break; // December
        default: month_code = 'Q'; break; // Default to August
    }

    // Format the contract code
    char contract[7];
    snprintf(contract, sizeof(contract),  "WIN%c%d", month_code, year);

    return std::string(contract);
}

// Improved function to determine the correct WDO contract code
std::string get_wdo_contract() {
    std::time_t t = std::time(nullptr);
    
    // Ajustar para horário brasileiro (UTC-3)
    t -= 3 * 3600; // 3 horas em segundos
    
    std::tm local_tm;
    gmtime_r(&t, &local_tm); // Usar gmtime_r pois já ajustamos o offset

    int year = local_tm.tm_year % 100;  // Get last two digits of year
    int month = local_tm.tm_mon + 1;    // tm_mon is 0-based
    int day = local_tm.tm_mday;

    // WDO contracts use month codes: F(Jan), G(Feb), H(Mar), J(Apr), K(May), M(Jun), Q(Aug), V(Oct), X(Nov), Z(Dec)
    // Note: WDO skips July (N) and September (U)
    
    // WDO contracts expire on the first business day of the month
    // We need to determine which contract is currently active
    // If we're in the first few days of the month, we might still be using the previous month's contract
    
    // For August (month 8), we should use 'Q' (not 'V')
    // Adjust the month selection logic
    int contract_month = month;
    
    // If we're past the 3rd day of the month, use next available contract month
    if (day > 3) {
        // Find next available contract month
        if (month == 7) contract_month = 8;      // July -> August (Q)
        else if (month == 8) contract_month = 10; // August -> October (V) 
        else if (month == 9) contract_month = 10; // September -> October (V)
        else contract_month = month + 1;
        
        if (contract_month > 12) {
            contract_month = 1;
            year++;
        }
    }

    // Get the correct month code based on contract month
    char month_code;
    switch(contract_month) {
        case 1: month_code = 'F'; break;  // January
        case 2: month_code = 'G'; break;  // February  
        case 3: month_code = 'H'; break;  // March
        case 4: month_code = 'J'; break;  // April
        case 5: month_code = 'K'; break;  // May
        case 6: month_code = 'M'; break;  // June
        case 8: month_code = 'Q'; break;  // August
        case 10: month_code = 'V'; break; // October
        case 11: month_code = 'X'; break; // November
        case 12: month_code = 'Z'; break; // December
        default: month_code = 'Q'; break; // Default to August
    }

    // Format the contract code
    char contract[7];
    snprintf(contract,sizeof(contract),  "WDO%c%d", month_code, year);

    return std::string(contract);
}

// Functions from gera_renko.c
char* extract_base_symbol(const char* full_symbol) {
    static char base_symbol[16];

    // Check if symbol starts with W (WIN/WDO cases)
    if (full_symbol[0] == 'W') {
        // Check second letter to determine if WIN or WDO
        if (full_symbol[1] == 'I') {
            strncpy(base_symbol, "WIN", sizeof(base_symbol) - 1 -1);
	    base_symbol[sizeof(base_symbol) - 1] = '\0';
        }
        else if (full_symbol[1] == 'D') {
            strncpy(base_symbol, "WDO", sizeof(base_symbol) - 1);
	    base_symbol[sizeof(base_symbol) - 1] = '\0';
        }
        else {
            strncpy(base_symbol, "W", sizeof(base_symbol) - 1); // fallbac
	    base_symbol[sizeof(base_symbol) - 1] = '\0';
        }
    }
    // Check for DI case
    else if (strncmp(full_symbol, "DI", 2) == 0) {
        strncpy(base_symbol, "DI", sizeof(base_symbol) -1);
	base_symbol[sizeof(base_symbol) - 1] = '\0';
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

TradeData parse_trade_message(const char* line) {
    TradeData trade = { 0 };
    char* token = NULL, * next_token = NULL;
    char* line_copy;
    int field = 0;

    // Skip "V:" prefix and get asset
    line_copy = _strdup(line + 2);
    token = strtok_r(line_copy, ":", &next_token);
    if (token) {
        // Extract base symbol from full contract code
        char* base_symbol = extract_base_symbol(token);
        
        // Debug: log symbol extraction for WDO
        if (strncmp(token, "WDO", 3) == 0) {
            static int wdo_symbol_counter = 0;
            if (wdo_symbol_counter++ % 100 == 0) {
                std::cerr << "[DEBUG] Símbolo WDO recebido: '" << token << "' -> extraído: '" << base_symbol << "' (contador: " << wdo_symbol_counter << ")" << std::endl;
            }
        }
        
        // Replace strncpy with strncpy_s
        strncpy(trade.asset, base_symbol, sizeof(trade.asset) - 1);
	trade.asset[sizeof(trade.asset) - 1] = '\0';
    }

    // Parse remaining fields
    while ((token = strtok_r(NULL, ":", &next_token)) != NULL) {
        field++;
        switch (field) {
        case 1: // Operation
            trade.operation = token[0];
            break;
        case 2: // Trade time
            // Replace strncpy with strncpy_s
            strncpy(trade.time,  token, sizeof(trade.time) - 1);
	    trade.time[sizeof(trade.time) - 1] = '\0'; 
            break;
        case 3: // Price
            trade.price = atof(token);
            break;
        case 4: // Buyer broker
            // Replace strncpy with strncpy_s
            strncpy(trade.buyer, token, sizeof(trade.buyer) - 1);
	    trade.buyer[sizeof(trade.buyer) - 1] = '\0'; 
            break;
        case 5: // Seller broker
            // Replace strncpy with strncpy_s
            strncpy(trade.seller, token, sizeof(trade.seller) - 1);
	    trade.seller[sizeof(trade.seller) - 1] = '\0'; 
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

// Initialize Renko files and variables
// Modify the initialize_renko_processing function to not keep files open
void initialize_renko_processing(const RenkoConfig* configs, int num_configs, const char* day) {
    // Prepare Renko output files for each symbol
    for (int s = 0; s < num_configs; s++) {
        for (int i = 0; i < configs[s].num_sizes; i++) {
            char renko_file[256];
            snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", day, configs[s].asset, configs[s].sizes[i]);
            
            // Create directory if it doesn't exist
            fs::create_directories("/home/grao/dados/renko_files/");
            
            // Open the file and keep it open
            renko_files[s][i] = fopen(renko_file, "a");
            if (!renko_files[s][i]) {
               perror("Erro ao abrir o arquivo Renko");
               continue;
            }
            
            // Write header only if file is new (check file size)
            fseek(renko_files[s][i], 0, SEEK_END);
            if (ftell(renko_files[s][i]) == 0) {
                fprintf(renko_files[s][i], "data,time,open,high,low,close\n");
                fflush(renko_files[s][i]);
            }
            
            // Initialize variables
            current_prices[s][i] = 0.0;
            high_prices[s][i] = 0.0;
            low_prices[s][i] = 9999999.0;
            brick_sizes[s][i] = configs[s].sizes[i] * configs[s].factor;
            last_trade_ids[s][i] = -1;
            trend_direction[s][i] = 0;
            
            // Initialize high/low tracking
            renko_tracking[s][i].brick_high = 0.0;
            renko_tracking[s][i].brick_low = 9999999.0;
        }
    }
}

// Add a helper function to write to Renko files
void write_to_renko_file(int config_index, int size_index, const char* day, const RenkoConfig* configs, 
                         const char* time_str, time_t timestamp, int msec, 
                         double open, double high, double low, double close) {
    // Use the already opened file from renko_files array
    if (!renko_files[config_index][size_index]) {
        // Tentar reabrir o arquivo automaticamente
        char renko_file[256];
        snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                day, configs[config_index].asset, configs[config_index].sizes[size_index]);
        
        std::cerr << "[AVISO] Arquivo Renko fechado, tentando reabrir: " << renko_file << std::endl;
        
        renko_files[config_index][size_index] = fopen(renko_file, "a");
        if (!renko_files[config_index][size_index]) {
            std::cerr << "[ERRO] Não foi possível reabrir arquivo Renko: " << renko_file << std::endl;
            perror("Erro ao reabrir arquivo Renko");
            return;
        }
        
        std::cerr << "[INFO] Arquivo Renko reaberto com sucesso: " << renko_file << std::endl;
    }
    
    // Write data to the already opened file
    int result = fprintf(renko_files[config_index][size_index], "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
            time_str, (long long)timestamp, msec, open, high, low, close);
    
    // Check if write was successful
    if (result < 0 || ferror(renko_files[config_index][size_index])) {
        std::cerr << "ERRO: Falha ao escrever no arquivo Renko [" << config_index << "][" << size_index << "]" << std::endl;
        
        // Clear error and try to reopen the file
        clearerr(renko_files[config_index][size_index]);
        fclose(renko_files[config_index][size_index]);
        
        // Reconstruct filename and reopen
        char renko_file[256];
        snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                day, configs[config_index].asset, configs[config_index].sizes[size_index]);
        
        renko_files[config_index][size_index] = fopen(renko_file, "a");
        if (renko_files[config_index][size_index] == NULL) {
            std::cerr << "ERRO CRÍTICO: Não foi possível reabrir arquivo Renko: " << renko_file << std::endl;
        } else {
            // Try to write again
            fprintf(renko_files[config_index][size_index], "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                    time_str, (long long)timestamp, msec, open, high, low, close);
        }
    }
    
    // Flush to ensure data is written immediately
    if (renko_files[config_index][size_index] != NULL) {
        fflush(renko_files[config_index][size_index]);
    }
}

// Modify process_trade_for_renko to use the new write function
// Define a structure to track state for each Renko configuration and size
typedef struct {
    double current_price;
    double high_price;
    double low_price;
    int trend_direction;
    long last_trade_id;
    char current_time_str[30];
} RenkoState;

// Global array to store state for each config and size
RenkoState** renko_states = NULL;

// Initialize the Renko states
void initialize_renko_states(const RenkoConfig* configs, int num_configs) {
    // Allocate memory for the states
    renko_states = (RenkoState**)malloc(num_configs * sizeof(RenkoState*));
    
    for (int s = 0; s < num_configs; s++) {
        renko_states[s] = (RenkoState*)malloc(configs[s].num_sizes * sizeof(RenkoState));
        
        for (int i = 0; i < configs[s].num_sizes; i++) {
            // Initialize state for each config and size
            renko_states[s][i].current_price = 0.0;
            renko_states[s][i].high_price = 0.0;
            renko_states[s][i].low_price = 9999999.0;
            renko_states[s][i].trend_direction = 0;
            renko_states[s][i].last_trade_id = -1;
            renko_states[s][i].current_time_str[0] = '\0';
        }
    }
}

// Free the Renko states
void cleanup_renko_states(const RenkoConfig* configs, int num_configs) {
    if (renko_states) {
        for (int s = 0; s < num_configs; s++) {
            free(renko_states[s]);
        }
        free(renko_states);
        renko_states = NULL;
    }
}

// Modified process_trade_for_renko function
void process_trade_for_renko(const char* line, const RenkoConfig* configs, int num_configs) {
    // Skip if not a trade message
    if (strncmp(line, "V:", 2) != 0) {
        return;
    }
    
    TradeData trade = parse_trade_message(line);
    
    // Skip if not an add operation
    if (trade.operation != 'A') return;
    
    // Get current date for file naming
    std::string current_date = get_current_date();
    
    // Parse time components
    int hour, min, sec, msec;
    if (sscanf(trade.time, "%2d%2d%2d%3d", &hour, &min, &sec, &msec) != 4) {
        return;
    }
    
    // Generate proper timestamp with milliseconds
    time_t now = time(NULL);
    
    // Ajustar para horário brasileiro (UTC-3)
    now -= 3 * 3600; // 3 horas em segundos
    
    struct tm tm_now_local;
    gmtime_r(&now, &tm_now_local); // Usar gmtime_r pois já ajustamos o offset
    
    // Use current date but with trade time
    struct tm trade_time = tm_now_local;
    trade_time.tm_hour = hour;
    trade_time.tm_min = min;
    trade_time.tm_sec = sec;
    
    // Convert to Unix timestamp
    time_t timestamp = mktime(&trade_time);
    
    // Find the configuration for the current asset
    int config_index = -1;
    for (int s = 0; s < num_configs; s++) {
        if (strcmp(configs[s].asset, trade.asset) == 0) {
            config_index = s;
            break;
        }
    }
    if (config_index == -1) {
        // Debug: log unknown assets
        static int unknown_asset_counter = 0;
        if (unknown_asset_counter++ % 100 == 0) {
            std::cerr << "[DEBUG] Asset desconhecido: " << trade.asset << " (contador: " << unknown_asset_counter << ")" << std::endl;
        }
        return; // Skip unknown assets
    }
    
    // Debug: log WDO trades
    if (strcmp(trade.asset, "WDO") == 0) {
        static int wdo_trade_counter = 0;
        if (wdo_trade_counter++ % 50 == 0) {
            std::cerr << "[DEBUG] Trade WDO processado: preço=" << trade.price << ", ID=" << trade.trade_id << " (contador: " << wdo_trade_counter << ")" << std::endl;
        }
    }
    
    // Process each Renko size for the current asset
    for (int i = 0; i < configs[config_index].num_sizes; i++) {
        // Get the state for this config and size
        RenkoState* state = &renko_states[config_index][i];
        
        // Skip if we've already processed this trade ID for this symbol and brick size
        if (trade.trade_id <= state->last_trade_id && state->last_trade_id != -1) {
            continue;
        }
        
        // Update the last processed trade ID
        state->last_trade_id = trade.trade_id;
        
        double brick_size = configs[config_index].sizes[i] * configs[config_index].factor;
        
        // Debug: log brick size calculation for WDO
        if (strcmp(trade.asset, "WDO") == 0) {
            static int wdo_brick_counter = 0;
            if (wdo_brick_counter++ % 200 == 0) {
                std::cerr << "[DEBUG] WDO[" << configs[config_index].sizes[i] << "] brick_size=" << brick_size << ", current_price=" << state->current_price << ", trade_price=" << trade.price << " (contador: " << wdo_brick_counter << ")" << std::endl;
            }
        }
        
        // Initialize price if this is the first trade
        if (state->current_price == 0.0) {
            state->current_price = trade.price;
            state->high_price = trade.price;
            state->low_price = trade.price;
            
            // Debug: log initial brick creation
            if (strcmp(trade.asset, "WDO") == 0) {
                std::cerr << "[DEBUG] Criando tijolo inicial WDO[" << configs[config_index].sizes[i] << "]: preço=" << trade.price << std::endl;
            }
            
            // Write the initial brick
            time_t unix_time = timestamp - 3 * 3600; // Ajustar para horário brasileiro
            struct tm tm_info;
            gmtime_r(&unix_time, &tm_info); // Usar gmtime_r pois já ajustamos o offset
            char time_str[20];
            strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
            
            // Write Renko brick using the helper function
            write_to_renko_file(config_index, i, current_date.c_str(), configs,
                               time_str, timestamp, msec,
                               trade.price, trade.price, trade.price, trade.price);


            strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
            continue;
        }
        
        // Update high/low prices
        if (trade.price > state->high_price) {
            state->high_price = trade.price;
        }
        if (trade.price < state->low_price) {
            state->low_price = trade.price;
        }
        
        // Initialize trend direction if not set
        if (state->trend_direction == 0) {
            state->trend_direction = 1;  // Start with up direction
        }
        
        double current_price = state->current_price;
        double high_price = state->high_price;
        double low_price = state->low_price;
        
        // Calculate how many bricks to generate based on direction
        if (state->trend_direction > 0) {
            // Current trend is up
            if (trade.price > current_price + brick_size) {
                // Continue uptrend - generate up bricks
                int num_bricks = (int)((trade.price - current_price) / brick_size);
                
                // Debug: log brick generation for WDO
                if (strcmp(trade.asset, "WDO") == 0) {
                    std::cerr << "[DEBUG] WDO[" << configs[config_index].sizes[i] << "] gerando " << num_bricks << " tijolos UP: preço atual=" << current_price << ", novo preço=" << trade.price << ", brick_size=" << brick_size << std::endl;
                }
                
                for (int j = 0; j < num_bricks; j++) {
                    double brick_open = current_price;
                    double brick_close = brick_open + brick_size;
                    
                    // Convert Unix timestamp to readable time format
                    time_t unix_time = timestamp - 3 * 3600; // Ajustar para horário brasileiro
                    struct tm tm_info;
                    gmtime_r(&unix_time, &tm_info); // Usar gmtime_r pois já ajustamos o offset
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick using the helper function
                    write_to_renko_file(config_index, i, current_date.c_str(), configs,
                                       time_str, timestamp, msec,
                                       brick_open, high_price, low_price, brick_close);

                    strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    current_price = brick_close;
                    high_price = brick_close;
                    low_price = brick_close;
                }
            } 
            else if (trade.price < current_price - (brick_size * 2)) {
                // Reversal from up to down - need at least 2 bricks to change direction
                state->trend_direction = -1;
                
                // Generate down bricks
                int num_bricks = (int)((current_price - trade.price) / brick_size);
                
                for (int j = 0; j < num_bricks; j++) {
                    double brick_open = current_price;
                    double brick_close = brick_open - brick_size;
                    
                    // Convert Unix timestamp to readable time format
                    time_t unix_time = timestamp - 3 * 3600; // Ajustar para horário brasileiro
                    struct tm tm_info;
                    gmtime_r(&unix_time, &tm_info); // Usar gmtime_r pois já ajustamos o offset
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick using the helper function
                    write_to_renko_file(config_index, i, current_date.c_str(), configs,
                                       time_str, timestamp, msec,
                                       brick_open, high_price, low_price, brick_close);

                    
                    strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    current_price = brick_close;
                    high_price = brick_close;
                    low_price = brick_close;
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
                    
                    // Convert Unix timestamp to readable time format
                    time_t unix_time = timestamp - 3 * 3600; // Ajustar para horário brasileiro
                    struct tm tm_info;
                    gmtime_r(&unix_time, &tm_info); // Usar gmtime_r pois já ajustamos o offset
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick using the helper function
                    write_to_renko_file(config_index, i, current_date.c_str(), configs,
                                       time_str, timestamp, msec,
                                       brick_open, high_price, low_price, brick_close);

                    
                    strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    current_price = brick_close;
                    high_price = brick_close;
                    low_price = brick_close;
                }
            } 
            else if (trade.price > current_price + (brick_size * 2)) {
                // Reversal from down to up - need at least 2 bricks to change direction
                state->trend_direction = 1;
                
                // Generate up bricks
                int num_bricks = (int)((trade.price - current_price) / brick_size);
                
                for (int j = 0; j < num_bricks; j++) {
                    double brick_open = current_price;
                    double brick_close = brick_open + brick_size;
                    
                    // Convert Unix timestamp to readable time format
                    time_t unix_time = timestamp - 3 * 3600; // Ajustar para horário brasileiro
                    struct tm tm_info;
                    gmtime_r(&unix_time, &tm_info); // Usar gmtime_r pois já ajustamos o offset
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick using the helper function
                    write_to_renko_file(config_index, i, current_date.c_str(), configs,
                                       time_str, timestamp, msec,
                                       brick_open, high_price, low_price, brick_close);

                    
                    strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    current_price = brick_close;
                    high_price = brick_close;
                    low_price = brick_close;
                }
            }
        }
        
        // Debug: log when no bricks are generated for WDO
        if (strcmp(trade.asset, "WDO") == 0) {
            static int wdo_no_brick_counter = 0;
            if (wdo_no_brick_counter++ % 500 == 0) {
                std::cerr << "[DEBUG] WDO[" << configs[config_index].sizes[i] << "] sem movimento suficiente: current=" << current_price << ", trade=" << trade.price << ", brick_size=" << brick_size << ", trend=" << state->trend_direction << " (contador: " << wdo_no_brick_counter << ")" << std::endl;
            }
        }
        
        // Update the state for this config and size
        state->current_price = current_price;
        state->high_price = high_price;
        state->low_price = low_price;
    }
}

// Remove the close_renko_files function since files are closed immediately after writing
void close_renko_files(const RenkoConfig* configs, int num_configs) {
    for (int s = 0; s < num_configs; s++) {
        for (int i = 0; i < configs[s].num_sizes; i++) {
            if (renko_files[s][i]) {
                fclose(renko_files[s][i]);
                renko_files[s][i] = NULL;
            }
        }
    }
}

// Modified connect_and_listen function to include Renko processing
using boost::asio::ip::tcp;
void connect_and_listen() {
    // Initialize renko_files array to NULL
    for (int s = 0; s < MAX_SYMBOLS; s++) {
        for (int i = 0; i < MAX_RENKO_SIZES; i++) {
            renko_files[s][i] = NULL;
        }
    }
    
    // Configure Renko settings for multiple symbols
    RenkoConfig configs[MAX_SYMBOLS] = {
        {.asset = "DI", .factor = 0.1, .num_sizes = 2, .sizes = {3, 5} },
        {.asset = "WDO", .factor = 0.5, .num_sizes = 3, .sizes = {5, 7, 10} },
        {.asset = "WIN", .factor = 5.0, .num_sizes = 3, .sizes = {10, 20, 30} }
    };
    
    // Get current date
    std::string date = get_current_date();
    

    
    if (is_time_to_stop()) {
        std::cerr << "Fora do Horario - Aguardando próximo dia de operação..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(60));
        return;
    }

    // Initialize Renko processing
    initialize_renko_processing(configs, MAX_SYMBOLS, date.c_str());
    
    // Initialize Renko states
    initialize_renko_states(configs, MAX_SYMBOLS);

    // Create files for data logging outside the connection loop
    std::string raw_filename = "/home/grao/dados/cedro_files/" + date + "_raw_data.txt";
    fs::create_directories("/home/grao/dados/cedro_files");
    std::ofstream raw_file(raw_filename, std::ios::app);

    std::string booking_filename = "/home/grao/dados/renko_files/" + date + "_booking_data.txt";
    fs::create_directories("/home/grao/dados/renko_files");
    std::ofstream booking_file(booking_filename, std::ios::app);

    while (!is_time_to_stop()) {
        try {
            io_context_type io_context;
            tcp::resolver resolver(io_context);
            //tcp::resolver::results_type endpoints = resolver.resolve("datafeed1.cedrotech.com", "81");
	        //auto endpoints = resolver.resolve("datafeed1.cedrotech.com", "81");
            tcp::resolver::query query("datafeed1.cedrotech.com", "81");
            auto endpoints = resolver.resolve(query);	    //
            tcp::socket socket(io_context);
            boost::asio::connect(socket, endpoints);

            // Configure socket for telnet-style communication
            socket.set_option(boost::asio::socket_base::keep_alive(true));
            socket.set_option(boost::asio::socket_base::send_buffer_size(1024));
            socket.set_option(boost::asio::socket_base::receive_buffer_size(1024));

            std::cout << "Conectado ao servidor!" << std::endl;
            const char start[] = { '\r', '\n' };
            boost::asio::write(socket, boost::asio::buffer(start, sizeof(start)));

            boost::asio::streambuf response;
            boost::system::error_code error;
            size_t length = boost::asio::read_until(socket, response, "Username:");
            if (error) throw boost::system::system_error(error);

            std::string initial_response = std::string(boost::asio::buffers_begin(response.data()), boost::asio::buffers_begin(response.data()) + length);
            response.consume(length);
            std::cout << "Resposta inicial: " << initial_response << std::endl;
            
            const char username[] = { 'g', 'u', 's', 't', 'a', 'v', 'o', 'f', 'm', '\r', '\n' };
            boost::asio::write(socket, boost::asio::buffer(username, sizeof(username)));
            size_t length2 = boost::asio::read_until(socket, response, "Password:");
            std::string resposta = std::string(boost::asio::buffers_begin(response.data()), boost::asio::buffers_begin(response.data()) + length2);
            response.consume(length2);
            std::cout << "Resposta: " << resposta << std::endl;

            const char password[] = { 'M', 'k', 'd', 't', '@', '3', '5', '1', '2', '5', '6', '\r', '\n' };
            boost::asio::write(socket, boost::asio::buffer(password, sizeof(password)));

            size_t length3 = boost::asio::read_until(socket, response, "You are connected");
            std::string login_response = std::string(boost::asio::buffers_begin(response.data()), boost::asio::buffers_begin(response.data()) + length3);
            response.consume(length3);
            std::cout << "Resposta após senha: " << login_response << std::endl;

            // Get current contract codes
            std::string win_contract = get_win_contract();
            std::string wdo_contract = get_wdo_contract();

            // Update commands with current contract codes
            std::string commands[] = {
                "BQT " + win_contract + "\n",
                "GQT " + win_contract + " S 10\n",
                "BQT " + wdo_contract + "\n",
                "GQT " + wdo_contract + " S \n",
                "GQT DI1F27 S\n",
                "BQT DI1F27\n"
            };

            std::cout << "Using contracts: " << win_contract << " and " << wdo_contract << std::endl;
            std::cout << "[DEBUG] Contrato WDO configurado: " << wdo_contract << std::endl;

            for (size_t i = 0; i < 6; ++i) {
                boost::asio::write(socket, boost::asio::buffer(commands[i]));
            }

            while (!is_time_to_stop()) {
            // Check file system health periodically
            static int health_check_counter = 0;
            if (health_check_counter++ % 1000 == 0) { // Check every 1000 iterations
                auto now = std::chrono::system_clock::now();
                auto time_t = std::chrono::system_clock::to_time_t(now);
                std::cout << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] Sistema ativo - iteração " << health_check_counter << std::endl;
                
                // Check if files are still writable
                if (raw_file.is_open() && raw_file.fail()) {
                    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] AVISO: raw_file em estado de erro, tentando recuperar..." << std::endl;
                    raw_file.clear();
                }
                else {
                    // Verificar se arquivos estão fechados e decidir ação
                    if (!raw_file.is_open() || !booking_file.is_open()) {
                        std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] ERRO CRÍTICO: Arquivos principais fechados!" << std::endl;
                        std::cerr << "raw_file: " << (raw_file.is_open() ? "ABERTO" : "FECHADO") << ", booking_file: " << (booking_file.is_open() ? "ABERTO" : "FECHADO") << std::endl;
                        
                        // Tentar reabrir os arquivos
                        if (!raw_file.is_open()) {
                            raw_file.open(raw_filename, std::ios::app);
                        }
                        if (!booking_file.is_open()) {
                            booking_file.open(booking_filename, std::ios::app);
                        }
                        
                        // Se ainda não conseguiu abrir, fechar o programa
                        if (!raw_file.is_open() || !booking_file.is_open()) {
                            std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] FALHA CRÍTICA: Não foi possível reabrir arquivos. Encerrando programa para reinício pelo supervisor." << std::endl;
                            
                            // Fechar arquivos Renko antes de sair
                            close_renko_files(configs, MAX_SYMBOLS);
                            cleanup_renko_states(configs, MAX_SYMBOLS);
                            
                            // Fechar arquivos principais se estiverem abertos
                            if (raw_file.is_open()) raw_file.close();
                            if (booking_file.is_open()) booking_file.close();
                            
                            std::exit(1); // Sair com código de erro para que o supervisor reinicie
                        }
                        
                        std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] Arquivos reabertos com sucesso." << std::endl;
                    }
                }
                if (booking_file.is_open() && booking_file.fail()) {
                    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] AVISO: booking_file em estado de erro, tentando recuperar..." << std::endl;
                    booking_file.clear();
                }
                
                // Check if files are still open
                std::cout << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] Status arquivos - raw: " << (raw_file.is_open() ? "ABERTO" : "FECHADO") 
                         << ", booking: " << (booking_file.is_open() ? "ABERTO" : "FECHADO") << std::endl;
            }
                
                // Verificar se a data mudou e recriar arquivos se necessário
                std::string current_date = get_current_date();
                if (current_date != date) {
                    std::cout << "Data mudou de " << date << " para " << current_date << ". Criando novos arquivos..." << std::endl;
                    
                    // Fechar arquivos antigos
                    if (raw_file.is_open()) {
                        raw_file.close();
                    }
                    if (booking_file.is_open()) {
                        booking_file.close();
                    }
                    
                    // Fechar arquivos Renko antigos
                    close_renko_files(configs, MAX_SYMBOLS);
                    
                    // Limpar estados Renko antigos
                    cleanup_renko_states(configs, MAX_SYMBOLS);
                    
                    // Atualizar data
                    date = current_date;
                    
                    // Reinicializar processamento Renko com nova data
                    initialize_renko_processing(configs, MAX_SYMBOLS, date.c_str());
                    initialize_renko_states(configs, MAX_SYMBOLS);
                    
                    // Criar novos arquivos com nova data
                    raw_filename = "/home/grao/dados/cedro_files/" + date + "_raw_data.txt";
                    booking_filename = "/home/grao/dados/renko_files/" + date + "_booking_data.txt";
                    
                    raw_file.open(raw_filename, std::ios::app);
                    booking_file.open(booking_filename, std::ios::app);
                    
                    if (!raw_file.is_open()) {
                        std::cerr << "Erro ao criar novo arquivo raw_file: " << raw_filename << std::endl;
                    }
                    if (!booking_file.is_open()) {
                        std::cerr << "Erro ao criar novo arquivo booking_file: " << booking_filename << std::endl;
                    }
                }
                
                // Verificar e reabrir arquivos se necessário
                if (!raw_file.is_open()) {
                    raw_file.open(raw_filename, std::ios::app);
                    if (!raw_file.is_open()) {
                        std::cerr << "Erro ao reabrir arquivo raw_file: " << raw_filename << std::endl;
                    }
                }
                if (!booking_file.is_open()) {
                    booking_file.open(booking_filename, std::ios::app);
                    if (!booking_file.is_open()) {
                        std::cerr << "Erro ao reabrir arquivo booking_file: " << booking_filename << std::endl;
                    }
                }

                boost::asio::streambuf reply_buf;
                boost::system::error_code error;

                // Read all available data
                size_t reply_length = socket.read_some(reply_buf.prepare(1024), error);
                if (error) {
                    if (error == boost::asio::error::eof) {
                        std::cerr << "Connection closed by server" << std::endl;
                    }
                    throw boost::system::system_error(error);
                }

                reply_buf.commit(reply_length);
                std::string response_data = std::string(boost::asio::buffers_begin(reply_buf.data()),
                    boost::asio::buffers_begin(reply_buf.data()) + reply_length);
                reply_buf.consume(reply_length);

                // Write raw data directly to file
                if (!response_data.empty()) {
                    raw_file << response_data;
                    
                    // Check if write was successful
                    if (raw_file.fail()) {
                        auto now = std::chrono::system_clock::now();
                        auto time_t = std::chrono::system_clock::to_time_t(now);
                        std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] ERRO: Falha ao escrever no arquivo raw_file: " << raw_filename << std::endl;
                        std::cerr << "Estado do arquivo - eof: " << raw_file.eof() << ", bad: " << raw_file.bad() << ", fail: " << raw_file.fail() << std::endl;
                        
                        raw_file.clear(); // Clear error flags
                        // Try to reopen the file
                        raw_file.close();
                        raw_file.open(raw_filename, std::ios::app);
                        if (!raw_file.is_open()) {
                            std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] ERRO CRÍTICO: Não foi possível reabrir raw_file" << std::endl;
                        } else {
                            std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] raw_file reaberto com sucesso" << std::endl;
                        }
                    } else {
                        raw_file.flush(); // Ensure data is written immediately
                        std::cout << "Received " << reply_length << " bytes of data" << std::endl;
                    }
                    
                    // Process the data for Renko generation
                    // Split the response data into lines
                    std::vector<std::string> lines;
                    boost::split(lines, response_data, boost::is_any_of("\n"));
                    
                    for (const auto& line : lines) {
                        if (line.empty()) continue;
                        
                        // Process booking data
                        if (line.rfind("B:", 0) == 0) {
                            booking_file << line << std::endl;
                            
                            // Check if booking write was successful
                            if (booking_file.fail()) {
                                auto now = std::chrono::system_clock::now();
                                auto time_t = std::chrono::system_clock::to_time_t(now);
                                std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] ERRO: Falha ao escrever no arquivo booking_file: " << booking_filename << std::endl;
                                std::cerr << "Estado do arquivo - eof: " << booking_file.eof() << ", bad: " << booking_file.bad() << ", fail: " << booking_file.fail() << std::endl;
                                
                                booking_file.clear(); // Clear error flags
                                // Try to reopen the file
                                booking_file.close();
                                booking_file.open(booking_filename, std::ios::app);
                                if (!booking_file.is_open()) {
                                    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] ERRO CRÍTICO: Não foi possível reabrir booking_file" << std::endl;
                                } else {
                                    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] booking_file reaberto com sucesso" << std::endl;
                                }
                            } else {
                                booking_file.flush();
                            }
                            
                            // Log successful writes periodically
                            static int booking_write_counter = 0;
                            if (booking_write_counter++ % 100 == 0) {
                                auto now = std::chrono::system_clock::now();
                                auto time_t = std::chrono::system_clock::to_time_t(now);
                                std::cout << "[" << std::put_time(std::localtime(&time_t), "%H:%M:%S") << "] booking_file: " << booking_write_counter << " escritas realizadas" << std::endl;
                            }
                            continue;
                        }
                        
                        // Process trade data for Renko generation
                        if (line.rfind("V:", 0) == 0) {
                            process_trade_for_renko(line.c_str(), configs, MAX_SYMBOLS);
                        }
                    }
                }
            }

           
            close_renko_files(configs, MAX_SYMBOLS);

        }
        catch (const std::exception& e) {
            std::cerr << "Erro de conexão: " << e.what() << std::endl;
            
            // Close Renko files before reconnecting
            close_renko_files(configs, MAX_SYMBOLS);

            if (raw_file.is_open()) {
                raw_file.close();
            }
            if (booking_file.is_open()) {
                booking_file.close();
            }
                        

            // Se estiver fora do horário, sai do loop atual
            if (is_time_to_stop()) {
                std::cerr << "Fora do horário de operação. Aguardando próximo dia..." << std::endl;
                return;
            }
            
            std::cerr << "Tentando reconectar em 5 segundos..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }
    }
    // Close Renko files
    close_renko_files(configs, MAX_SYMBOLS);
    // Close the files when exiting
    if (raw_file.is_open()) {
        raw_file.close();
    }
    if (booking_file.is_open()) {
        booking_file.close();
    }
    
    // Make sure to close all Renko files when exiting
    close_renko_files(configs, MAX_SYMBOLS);
}

int main() {
    while (true) {
        try {
            connect_and_listen();
            std::cout << "Sessão encerrada. Reiniciando em 5 segundos..." << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Erro na sessão principal: " << e.what() << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    return 0;
}

