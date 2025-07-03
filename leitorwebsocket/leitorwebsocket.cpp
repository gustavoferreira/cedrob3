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
    std::tm local_tm;
    localtime_r(&t, &local_tm);
    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d", &local_tm);
    return std::string(buffer);
}

bool is_time_to_stop() {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm local_tm;
    localtime_r(&now, &local_tm);
    return (local_tm.tm_hour >= 19 or local_tm.tm_hour < 9);
}

void log_response(const std::string& response, const std::string& symbol) {
    std::string date = get_current_date();
    std::string type = (response.rfind("V:", 0) == 0) ? "quote" : "booking";
    std::string filename = "/home/grao/dados/cedro_files/" + date + "_" + symbol + "_" + type + ".txt";

    fs::create_directories("/home/grao/dados/cedro_files/");
    std::ofstream log_file(filename, std::ios::app);
    if (log_file.is_open()) {
        log_file << response << std::endl;
        log_file.close();
    }
}


// Add this function to determine the correct WIN contract code
std::string get_win_contract() {
    std::time_t t = std::time(nullptr);
    std::tm local_tm;
    localtime_r(&t, &local_tm);

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

    // Get the correct month code
    char month_code = month_codes[month - 1];

    // Format the contract code
    char contract[7];
    snprintf(contract, sizeof(contract),  "WIN%c%d", month_code, year);

    return std::string(contract);
}

// Improved function to determine the correct WDO contract code
std::string get_wdo_contract() {
    std::time_t t = std::time(nullptr);
    std::tm local_tm;
    localtime_r(&t, &local_tm);

    int year = local_tm.tm_year % 100;  // Get last two digits of year
    int month = local_tm.tm_mon + 1;    // tm_mon is 0-based
    int day = local_tm.tm_mday;

    // WDO contracts use month codes: F, G, H, J, K, M, Q, V, X, Z, F, G
    char month_codes[] = { 'F', 'G', 'H', 'J', 'K', 'M', 'Q', 'V', 'X', 'Z', 'F', 'G' };

    // For WDO, if we're on or past the 1st day of the month, we need to use the next month's contract
    // since the contract expires on the first business day of the month
    if (day >= 1) {
        // Move to next month's contract
        month++;
        if (month > 12) {
            month = 1;
            year++;
        }
    }

    // Get the correct month code
    char month_code = month_codes[month - 1];

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
            
            // Open the file just to create it and write the header

            FILE* temp_file = fopen(renko_file, "w");
            if (!temp_file) {
               perror("Erro ao abrir o arquivo");
               // lidar com o erro aqui
	       continue;
            }


            
            // Write header and close immediately
            fprintf(temp_file, "data,time,open,high,low,close\n");
            fclose(temp_file);
            
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
    char renko_file[256];
    snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
              day, configs[config_index].asset, configs[config_index].sizes[size_index]);
    
    // Open file in append mode
    FILE* temp_file = fopen(renko_file, "w");
    if (!temp_file) {
       perror("Erro ao abrir o arquivo");
       // lidar com o erro aqui
       return;
    }


    
    // Write data
    fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
            time_str, (long long)timestamp, msec, open, high, low, close);
    
    // Close file immediately
    fclose(temp_file);
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
    struct tm tm_now_local;
    localtime_r(&now, &tm_now_local);
    
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
        return; // Skip unknown assets
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
        
        // Initialize price if this is the first trade
        if (state->current_price == 0.0) {
            state->current_price = trade.price;
            state->high_price = trade.price;
            state->low_price = trade.price;
            
            // Write the initial brick
            time_t unix_time = timestamp;
            struct tm tm_info;
            localtime_r(&unix_time, &tm_info);
            char time_str[20];
            strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
            
            // Write Renko brick with formatted date and time
            char renko_file[256];
            snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                      current_date.c_str(), configs[config_index].asset, configs[config_index].sizes[i]);



            FILE* temp_file = fopen(renko_file, "w");
            if (!temp_file) {
               perror("Erro ao abrir o arquivo");
               fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                        time_str, (long long)timestamp, msec,
                        trade.price, trade.price, trade.price, trade.price);
               fclose(temp_file);

            }


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
                
                for (int j = 0; j < num_bricks; j++) {
                    double brick_open = current_price;
                    double brick_close = brick_open + brick_size;
                    
                    // Convert Unix timestamp to readable time format
                    time_t unix_time = timestamp;
                    struct tm tm_info;
                    localtime_r(&unix_time, &tm_info);
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick with formatted date and time
                    char renko_file[256];
                    snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                              current_date.c_str(), configs[config_index].asset, configs[config_index].sizes[i]);
                    

                    FILE* temp_file = fopen(renko_file, "w");
                    if (!temp_file) {
                        perror("Erro ao abrir o arquivo");
                        // lidar com o erro aqui
                        fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str, (long long)timestamp, msec,
                                brick_open, high_price, low_price, brick_close);
                        fclose(temp_file);

		    }

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
                    time_t unix_time = timestamp;
                    struct tm tm_info;
                    localtime_r(&unix_time, &tm_info);
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick with formatted date and time
                    char renko_file[256];
                    snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                              current_date.c_str(), configs[config_index].asset, configs[config_index].sizes[i]);
                    


                   FILE* temp_file = fopen(renko_file, "w");
                   if (!temp_file) {
                       perror("Erro ao abrir o arquivo");
                       // lidar com o erro aqui
                       fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str, (long long)timestamp, msec,
                                brick_open, high_price, low_price, brick_close);
                       fclose(temp_file);

		   }

                    
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
                    time_t unix_time = timestamp;
                    struct tm tm_info;
                    localtime_r(&unix_time, &tm_info);
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick with formatted date and time
                    char renko_file[256];
                    snprintf(renko_file, sizeof(renko_file), "/home/dados/renko_files/%s_%s_renko_%d.csv", 
                              current_date.c_str(), configs[config_index].asset, configs[config_index].sizes[i]);
                    

                    FILE* temp_file = fopen(renko_file, "w");
                    if (!temp_file) {
                        perror("Erro ao abrir o arquivo");
                        // lidar com o erro aquia
                        fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str, (long long)timestamp, msec,
                                brick_open, high_price, low_price, brick_close);
                        fclose(temp_file);
			
                    }

                    
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
                    time_t unix_time = timestamp;
                    struct tm tm_info;
                    localtime_r(&unix_time, &tm_info);
                    char time_str[20];
                    strftime(time_str, sizeof(time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    
                    // Write Renko brick with formatted date and time
                    char renko_file[256];
                    snprintf(renko_file, sizeof(renko_file), "/home/grao/dados/renko_files/%s_%s_renko_%d.csv", 
                              current_date.c_str(), configs[config_index].asset, configs[config_index].sizes[i]);
                    

                    FILE* temp_file = fopen(renko_file, "w");
                    if (!temp_file) {
                        perror("Erro ao abrir o arquivo");
                        // lidar com o erro aquia
			fprintf(temp_file, "%s,%lld.%03d,%.2f,%.2f,%.2f,%.2f\n",
                                time_str, (long long)timestamp, msec,
                                brick_open, high_price, low_price, brick_close);
                        fclose(temp_file);
                    }

                    
                    strftime(state->current_time_str, sizeof(state->current_time_str), "%Y%m%d %H:%M:%S", &tm_info);
                    current_price = brick_close;
                    high_price = brick_close;
                    low_price = brick_close;
                }
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
    // Configure Renko settings for multiple symbols
    RenkoConfig configs[MAX_SYMBOLS] = {
        {.asset = "DI", .factor = 0.1, .num_sizes = 2, .sizes = {3, 5} },
        {.asset = "WDO", .factor = 0.5, .num_sizes = 3, .sizes = {5, 7, 10} },
        {.asset = "WIN", .factor = 5.0, .num_sizes = 3, .sizes = {10, 20, 30} }
    };
    
    // Get current date
    std::string date = get_current_date();
    
    // Initialize Renko processing
    initialize_renko_processing(configs, MAX_SYMBOLS, date.c_str());
    
    // Initialize Renko states - ADD THIS LINE
    initialize_renko_states(configs, MAX_SYMBOLS);
    
    while (is_time_to_stop()) {
        std::cerr << " Fora do Horario- Tentando novamente em 10 segundos..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

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

            for (size_t i = 0; i < 6; ++i) {
                boost::asio::write(socket, boost::asio::buffer(commands[i]));
            }

            // Create a file for raw data
            std::string raw_filename = "/home/grao/dados/cedro_files/" + date + "_raw_data.txt";
            fs::create_directories("/home/grao/dados/cedro_files");
            std::ofstream raw_file(raw_filename, std::ios::app);

            // Create a file for booking data
            std::string booking_filename = "/home/grao/dados/renko_files/" + date + "_booking_data.txt";
            fs::create_directories("/home/grao/dados/renko_files");
            std::ofstream booking_file(booking_filename, std::ios::app);

            while (!is_time_to_stop()) {
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
                if (raw_file.is_open() && !response_data.empty()) {
                    raw_file << response_data;
                    raw_file.flush(); // Ensure data is written immediately
                    std::cout << "Received " << reply_length << " bytes of data" << std::endl;
                    
                    // Process the data for Renko generation
                    // Split the response data into lines
                    std::vector<std::string> lines;
                    boost::split(lines, response_data, boost::is_any_of("\n"));
                    
                    for (const auto& line : lines) {
                        if (line.empty()) continue;
                        
                        // Process booking data
                        if (line.rfind("B:", 0) == 0) {
                            if (booking_file.is_open()) {
                                booking_file << line << std::endl;
                                booking_file.flush();
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

            // Close the files when done
            if (raw_file.is_open()) {
                raw_file.close();
            }
            if (booking_file.is_open()) {
                booking_file.close();
            }
            
            // Close Renko files
            close_renko_files(configs, MAX_SYMBOLS);

        }
        catch (std::exception& e) {
            std::cerr << "Erro: " << e.what() << " - Tentando reconectar em 5 segundos..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
            
            // Close Renko files before reconnecting
            close_renko_files(configs, MAX_SYMBOLS);
        }
    }
    
    // Make sure to close all Renko files when exiting
    close_renko_files(configs, MAX_SYMBOLS);
}

int main() {
    connect_and_listen();
    return 0;
}
