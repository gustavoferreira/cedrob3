//g++-11 -std=c++17 leitorwebsocket.cpp -lboost_system -lpthread
#include <iostream>
#include <boost/asio.hpp>
#include <fstream>
#include <ctime>
#include <filesystem>
#include <chrono>
#include <thread>
#include <boost/algorithm/string.hpp>
#include <cmath>
#include <iomanip>
#include <atomic>
#include <csignal>
#include <regex>

//#using boost::asio::ip::tcp;
namespace fs = std::filesystem;

static std::atomic_bool g_stop{false};
static void handle_signal(int) { g_stop.store(true); }

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

#ifdef _WIN32
// Windows compatibility
void gmtime_r(const time_t *timep, struct tm *result) {
    gmtime_s(result, timep);
}
#else
#define _strdup strdup
#endif

// Configurable output directory
std::string get_output_dir() {
#ifdef _WIN32
    return "c:/cedrob3/dados/cedro_files/";
#else
    return "/home/grao/dados/cedro_files/";
#endif
}

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

// Timestamp local (UTC-3) para prefixar cada linha gravada
std::string get_current_datetime_str() {
    std::time_t t = std::time(nullptr);
    // Ajustar para horário brasileiro (UTC-3)
    t -= 3 * 3600;
    std::tm local_tm;
    gmtime_r(&t, &local_tm);
    char buffer[20];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &local_tm);
    return std::string(buffer);
}

// Segundos do dia (UTC-3) para comparação rápida com timestamp do evento no payload
int get_local_seconds_of_day() {
    auto now_tp = std::chrono::system_clock::now();
    auto now = std::chrono::system_clock::to_time_t(now_tp);
    // Ajuste UTC-3
    now -= 3 * 3600;
    std::tm local_tm;
    gmtime_r(&now, &local_tm);
    return local_tm.tm_hour * 3600 + local_tm.tm_min * 60 + local_tm.tm_sec;
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
    std::string filename = get_output_dir() + date + "_" + symbol + "_" + type + ".txt";

    try {
        // Create directories with error checking
        std::error_code ec;
        fs::create_directories(get_output_dir(), ec);
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

    // WIN contracts only exist in even months (Feb, Apr, Jun, Aug, Oct, Dec)
    // If we're in an odd month, move to next even month
    if (month % 2 == 1) {
        month += 1;
        if (month > 12) {
            month = 2;  // February of next year
            year++;
        }
    } else {
        // We're in an even month, check if we need to roll to next contract
        // Calculate the Wednesday closest to the 15th
        std::tm temp_tm = local_tm;
        temp_tm.tm_mday = 15;
        std::mktime(&temp_tm);  // Normalize the date
        
        // Find Wednesday closest to 15th
        int day_of_week = temp_tm.tm_wday;  // 0=Sunday, 3=Wednesday
        int days_to_wednesday;
        
        if (day_of_week <= 3) {
            days_to_wednesday = 3 - day_of_week;  // Forward to Wednesday
        } else {
            days_to_wednesday = 3 - day_of_week + 7;  // Next Wednesday
        }
        
        int expiration_day = 15 + days_to_wednesday;
        
        // Adjust if expiration day goes beyond month end
        if (expiration_day > 31) {
            expiration_day -= 7;  // Previous Wednesday
        }
        
        // If current day is past expiration day, roll to next contract
        if (day >= expiration_day) {
            month += 2;
            if (month > 12) {
                month -= 12;
                year++;
            }
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
    if (day >= 1) {
        // Find next available contract month
        contract_month = month + 1;
        
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
        case 7: month_code = 'N'; break;  // August
        case 8: month_code = 'Q'; break;  // August
        case 9: month_code = 'U'; break;  // August
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



// -----------------------------
// Dual-mode: live socket OR rebuild from raw_data.txt
// -----------------------------
struct ProgramArgs {
    std::string raw_file;   // se informado, roda em modo offline (rebuild)
    std::string out_dir;    // diretório de saída (default = get_output_dir())
    std::string date;       // YYYYMMDD (opcional; tenta inferir do nome/primeira linha)
    bool overwrite = false; // truncar arquivos de saída
    bool help = false;
};

static std::string ensure_trailing_slash(std::string p) {
    if (p.empty()) return p;
    if (p.back() != '/' && p.back() != '\\') p.push_back('/');
    return p;
}

static bool all_digits(const std::string& s) {
    for (char c : s) if (c < '0' || c > '9') return false;
    return !s.empty();
}

static bool is_yyyymmdd(const std::string& s) {
    return s.size() == 8 && all_digits(s);
}

static bool starts_with_data_prefix(const std::string& s) {
    return (s.rfind("B:", 0) == 0) || (s.rfind("V:", 0) == 0) || (s.rfind("T:", 0) == 0) || (s.rfind("Z:", 0) == 0);
}

static std::string infer_date_from_filename(const std::string& path) {
    std::string base = fs::path(path).filename().string();
    // procura qualquer sequência de 8 dígitos no nome
    for (size_t i = 0; i + 8 <= base.size(); ++i) {
        std::string sub = base.substr(i, 8);
        if (is_yyyymmdd(sub)) return sub;
    }
    return "";
}

static std::string infer_date_from_first_line(const std::string& raw_file) {
    std::ifstream in(raw_file);
    if (!in.is_open()) return "";
    std::string line;
    if (!std::getline(in, line)) return "";
    // formato típico: YYYYMMDD_HHMMSS,reply_len,delta_ms,payload...
    size_t p1 = line.find(',');
    if (p1 == std::string::npos) return "";
    std::string ts = line.substr(0, p1);
    if (ts.size() >= 8 && all_digits(ts.substr(0, 8))) return ts.substr(0, 8);
    return "";
}

static ProgramArgs parse_args(int argc, char** argv) {
    ProgramArgs a;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            a.help = true;
            return a;
        } else if (arg == "--raw-file" || arg == "--raw") {
            if (i + 1 < argc) a.raw_file = argv[++i];
        } else if (arg == "--out-dir") {
            if (i + 1 < argc) a.out_dir = argv[++i];
        } else if (arg == "--date") {
            if (i + 1 < argc) a.date = argv[++i];
        } else if (arg == "--overwrite") {
            a.overwrite = true;
        }
    }
    return a;
}

static void print_help(const char* prog) {
    std::cerr
        << "Uso:\n"
        << "  " << prog << "                 # modo live (socket)\n"
        << "  " << prog << " --raw-file <raw_data.txt> [--out-dir <dir>] [--date YYYYMMDD] [--overwrite]\n\n"
        << "Exemplos:\n"
        << "  " << prog << " --raw-file /home/grao/dados/cedro_files/20260107_raw_data.txt --overwrite\n"
        << "  " << prog << " --raw-file /home/grao/dados/cedro_files/20260107_raw_data.txt --out-dir /home/grao/dados/cedro_files/\n";
}

static void rebuild_from_raw(const ProgramArgs& args) {
    if (args.raw_file.empty()) {
        std::cerr << "[ERRO] --raw-file não informado.\n";
        return;
    }
    std::string out_dir = args.out_dir.empty() ? get_output_dir() : args.out_dir;
    out_dir = ensure_trailing_slash(out_dir);

    std::string date = args.date;
    if (!is_yyyymmdd(date)) date = infer_date_from_filename(args.raw_file);
    if (!is_yyyymmdd(date)) date = infer_date_from_first_line(args.raw_file);
    if (!is_yyyymmdd(date)) date = get_current_date();

    std::error_code ec;
    fs::create_directories(out_dir, ec);
    if (ec) {
        std::cerr << "[ERRO] Não foi possível criar diretório de saída: " << out_dir << " (" << ec.message() << ")\n";
        return;
    }

    std::ios_base::openmode mode = std::ios::out | std::ios::app;
    if (args.overwrite) mode = std::ios::out | std::ios::trunc;

    std::string b_filename = out_dir + date + "_B.txt";
    std::string v_filename = out_dir + date + "_V.txt";
    std::string t_filename = out_dir + date + "_T.txt";
    std::string z_filename = out_dir + date + "_Z.txt";
    std::string o_filename = out_dir + date + "_orphans.txt";

    std::ofstream b_out(b_filename, mode);
    std::ofstream v_out(v_filename, mode);
    std::ofstream t_out(t_filename, mode);
    std::ofstream z_out(z_filename, mode);
    std::ofstream o_out(o_filename, mode);

    if (!b_out.is_open() || !v_out.is_open() || !t_out.is_open() || !z_out.is_open()) {
        std::cerr << "[ERRO] Falha ao abrir arquivos de saída em: " << out_dir << "\n";
        return;
    }

    std::ifstream in(args.raw_file);
    if (!in.is_open()) {
        std::cerr << "[ERRO] Falha ao abrir raw_file: " << args.raw_file << "\n";
        return;
    }

    std::string pending_ts, pending_len, pending_delta;
    std::string pending_payload;
    char pending_type = 0; // 'B','V','T','Z'
    long long n_out_b=0, n_out_v=0, n_out_t=0, n_out_z=0, n_orphans=0, n_lines=0;

    auto flush_pending = [&]() {
        if (pending_payload.empty() || pending_type == 0) return;
        std::ostream* out = nullptr;
        switch (pending_type) {
            case 'B': out = &b_out; break;
            case 'V': out = &v_out; break;
            case 'T': out = &t_out; break;
            case 'Z': out = &z_out; break;
            default: out = nullptr; break;
        }
        if (out) {
            (*out) << pending_ts << "," << pending_len << "," << pending_delta << "," << pending_payload << "\n";
            if (pending_type == 'B') n_out_b++;
            else if (pending_type == 'V') n_out_v++;
            else if (pending_type == 'T') n_out_t++;
            else if (pending_type == 'Z') n_out_z++;
        }
        pending_ts.clear(); pending_len.clear(); pending_delta.clear();
        pending_payload.clear(); pending_type = 0;
    };

    std::string line;
    while (std::getline(in, line)) {
        n_lines++;
        if (line.empty()) continue;

        // Parse 4 colunas: ts, reply_len, delta_ms, payload...
        size_t p1 = line.find(',');
        if (p1 == std::string::npos) { n_orphans++; if (o_out.is_open()) o_out << line << "\n"; continue; }
        size_t p2 = line.find(',', p1 + 1);
        if (p2 == std::string::npos) { n_orphans++; if (o_out.is_open()) o_out << line << "\n"; continue; }
        size_t p3 = line.find(',', p2 + 1);
        if (p3 == std::string::npos) { n_orphans++; if (o_out.is_open()) o_out << line << "\n"; continue; }

        std::string ts = line.substr(0, p1);
        std::string reply_len = line.substr(p1 + 1, p2 - (p1 + 1));
        std::string delta_ms = line.substr(p2 + 1, p3 - (p2 + 1));
        std::string payload = line.substr(p3 + 1);

        if (payload.empty()) continue;

        // Regras:
        // 1) Se começar com B:/V:/T:/Z: => novo registro (flush do anterior)
        // 2) Se NÃO começar, mas já existe pending => é continuação (TCP quebrou no meio), concatenar.
        // 3) Se NÃO começar e não existe pending => orphan/ruído (handshake etc), descartar (opcionalmente logar).
        if (starts_with_data_prefix(payload)) {
            flush_pending();
            pending_ts = ts;
            pending_len = reply_len;
            pending_delta = delta_ms;
            pending_payload = payload;
            pending_type = payload[0];
        } else if (!pending_payload.empty()) {
            pending_payload += payload;
        } else {
            n_orphans++;
            if (o_out.is_open()) o_out << line << "\n";
        }
    }
    flush_pending();

    b_out.flush(); v_out.flush(); t_out.flush(); z_out.flush();
    if (o_out.is_open()) o_out.flush();

    std::cerr << "[OK] Rebuild finalizado.\n"
              << "  raw_file: " << args.raw_file << "\n"
              << "  date:     " << date << "\n"
              << "  out_dir:  " << out_dir << "\n"
              << "  lines:    " << n_lines << "\n"
              << "  B: " << n_out_b << "  V: " << n_out_v << "  T: " << n_out_t << "  Z: " << n_out_z << "\n"
              << "  orphans:  " << n_orphans << " (salvos em " << o_filename << ")\n";
}


using boost::asio::ip::tcp;
void connect_and_listen() {
    std::string date = get_current_date();
    std::string raw_filename = get_output_dir() + date + "_raw_data.txt";
    fs::create_directories(get_output_dir());
    std::ofstream raw_file(raw_filename, std::ios::app);
    // Arquivos separados por tipo de registro (B, V, T)
    std::string b_filename = get_output_dir() + date + "_B.txt";
    std::string v_filename = get_output_dir() + date + "_V.txt";
    std::string t_filename = get_output_dir() + date + "_T.txt";
    std::string z_filename = get_output_dir() + date + "_Z.txt";
    std::ofstream b_file;
    std::ofstream v_file;
    std::ofstream t_file;
    std::ofstream z_file;

    if (!raw_file.is_open()) {
        std::error_code ec;
        fs::create_directories(get_output_dir(), ec);
        raw_file.open(raw_filename, std::ios::app);
        if (!raw_file.is_open()) {
            std::cerr << "Erro crítico: arquivo raw não pôde ser aberto: " << raw_filename << std::endl;
        }
    }

    if (g_stop.load()) { return; }

    if (is_time_to_stop()) {
        std::cerr << "Fora do Horario - Aguardando próximo dia de operação..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(60));
        return;
    }

    while (!is_time_to_stop() && !g_stop.load()) {
            // Buffer de escrita em lote
            std::string batch_buffer;
            int batch_count = 0;
            // Buffers por tipo
            std::string b_batch;
            std::string v_batch;
            std::string t_batch;
            std::string z_batch;
            auto last_flush = std::chrono::steady_clock::now();
        std::chrono::steady_clock::time_point last_record_tp;
        bool has_last_record = false;
        std::string rx_pending; // acumula bytes entre read_some para não quebrar linhas
	
        try {
            io_context_type io_context;
            tcp::resolver resolver(io_context);
            tcp::resolver::query query("datafeed1.cedrotech.com", "81");
            auto endpoints = resolver.resolve(query);
            tcp::socket socket(io_context);
            boost::asio::connect(socket, endpoints);

            socket.set_option(boost::asio::socket_base::keep_alive(true));
            // Reduz latência em envios pequenos
            socket.set_option(boost::asio::ip::tcp::no_delay(true));
            // Buffers maiores para acelerar drenagem de backlog
            socket.set_option(boost::asio::socket_base::send_buffer_size(4096));
            socket.set_option(boost::asio::socket_base::receive_buffer_size(65536));

            std::cout << "Conectado ao servidor!" << std::endl;
            const char start[] = { '\r', '\n' };
            boost::asio::write(socket, boost::asio::buffer(start, sizeof(start)));

            // Handshake de login
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

            // Assina contratos atuais
            std::string win_contract = get_win_contract();
            std::string wdo_contract = get_wdo_contract();
            // Envia comandos em batch com CRLF para evitar problemas de buffer
            std::string cmd_batch;
            cmd_batch.reserve(256);
            cmd_batch += "BQT " + win_contract + " \r\n";
            cmd_batch += "BQT " + wdo_contract + " \r\n";
            cmd_batch += "GQT " + win_contract + " S \r\n";
            cmd_batch += "GQT " + wdo_contract + " S \r\n";
            cmd_batch += "SQT " + win_contract + " \r\n";
            cmd_batch += "SQT " + wdo_contract + " \r\n";
            cmd_batch += "SAB " + win_contract + " \r\n";
            cmd_batch += "SAB " + wdo_contract + " \r\n";
	    
            cmd_batch += "SQT DI1F27 \r\n";

            std::cout << "Enviando comandos:\n" << cmd_batch << std::endl;
            // Primeiro tenta envio em batch
            boost::system::error_code write_ec;
            boost::asio::write(socket, boost::asio::buffer(cmd_batch), write_ec);
            if (write_ec) {
                std::cerr << "Falha no envio em batch: " << write_ec.message() << ". Tentando envio linha a linha..." << std::endl;
                // Fallback: enviar linha a linha
                auto send_line = [&](const std::string& line){
                    std::string l = line;
                    if (l.empty() || l.back() != '\n') l += "\r\n";
                    boost::asio::write(socket, boost::asio::buffer(l));
                };
                //send_line("BQT " + win_contract);
		//
		send_line("BQT " + win_contract );
  	        send_line("BQT " + win_contract ); 
                //send_line("BQT " + wdo_contract);
                send_line("GQT " + wdo_contract + " S");
                send_line("GQT " + win_contract + " S");

		send_line("SQT " + wdo_contract );
                send_line("SQT " + win_contract );

                send_line("SAB " + wdo_contract );
                send_line("SAB " + win_contract );

		

                //send_line("BQT DI1F27");
                send_line("SQT DI1F27");
            }

            // Buffer de escrita em lote já definido fora do try

            // Loop principal: lê e grava dados brutos
            while (!is_time_to_stop() && !g_stop.load()) {
                // Rotaciona arquivo quando muda a data
                std::string current_date = get_current_date();
                if (current_date != date) {
                    date = current_date;
                    // Flush pendente antes de trocar de arquivo
                    if (!batch_buffer.empty() && raw_file.is_open()) {
                        raw_file << batch_buffer;
                        raw_file.flush();
                        batch_buffer.clear();
                        batch_count = 0;
                    }
                    // Flush pendente dos buffers por tipo
                    if (!b_batch.empty()) {
                        if (!b_file.is_open()) b_file.open(b_filename, std::ios::app);
                        if (b_file.is_open()) { b_file << b_batch; b_file.flush(); b_batch.clear(); }
                    }
                    if (!v_batch.empty()) {
                        if (!v_file.is_open()) v_file.open(v_filename, std::ios::app);
                        if (v_file.is_open()) { v_file << v_batch; v_file.flush(); v_batch.clear(); }
                    }
                    if (!t_batch.empty()) {
                        if (!t_file.is_open()) t_file.open(t_filename, std::ios::app);
                        if (t_file.is_open()) { t_file << t_batch; t_file.flush(); t_batch.clear(); }
                    }
                    if (!z_batch.empty()) {
                        if (!z_file.is_open()) z_file.open(z_filename, std::ios::app);
                        if (z_file.is_open()) { z_file << z_batch; z_file.flush(); z_batch.clear(); }
                    }
                    if (raw_file.is_open()) raw_file.close();
                    if (b_file.is_open()) b_file.close();
                    if (v_file.is_open()) v_file.close();
                    if (t_file.is_open()) t_file.close();
                    if (z_file.is_open()) z_file.close();
                    raw_filename = get_output_dir() + date + "_raw_data.txt";
                    b_filename = get_output_dir() + date + "_B.txt";
                    v_filename = get_output_dir() + date + "_V.txt";
                    t_filename = get_output_dir() + date + "_T.txt";
                    z_filename = get_output_dir() + date + "_Z.txt";
                    std::error_code ec;
                    fs::create_directories(get_output_dir(), ec);
                    raw_file.open(raw_filename, std::ios::app);
                    if (!raw_file.is_open()) {
                        std::cerr << "Erro ao reabrir arquivo raw: " << raw_filename << std::endl;
                    }
                }

                boost::asio::streambuf reply_buf;
                boost::system::error_code read_error;
                size_t reply_length = socket.read_some(reply_buf.prepare(65536), read_error);
                if (read_error) {
                    if (read_error == boost::asio::error::eof) {
                        std::cerr << "Connection closed by server" << std::endl;
                        break;
                    }
                    throw boost::system::system_error(read_error);
                }

                reply_buf.commit(reply_length);
                std::string response_data(
                    boost::asio::buffers_begin(reply_buf.data()),
                    boost::asio::buffers_begin(reply_buf.data()) + reply_length
                );
                reply_buf.consume(reply_length);

                if (!response_data.empty()) {
                    // NOTE: TCP pode quebrar uma linha no meio. Acumulamos bytes em rx_pending e só processamos linhas completas (terminadas em \n).
                    rx_pending.append(response_data);

                    size_t start_pos = 0;
                    while (true) {
                        size_t nl = rx_pending.find('\n', start_pos);
                        if (nl == std::string::npos) break;

                        std::string line = rx_pending.substr(start_pos, nl - start_pos);
                        start_pos = nl + 1;
                        if (!line.empty() && line.back() == '\r') line.pop_back();

                        if (line.empty()) continue;
                        //const std::string ts = get_current_datetime_str();
                        //batch_buffer += ts + " " + line + "\n";
                        //batch_count++;
                        const std::string ts = get_current_datetime_str();
                        auto now_line = std::chrono::steady_clock::now();
                        long long delta_ms = has_last_record ? std::chrono::duration_cast<std::chrono::milliseconds>(now_line - last_record_tp).count() : 0;
                        last_record_tp = now_line;
                        has_last_record = true;
                        batch_buffer += ts + "," + std::to_string(reply_length) + "," + std::to_string(delta_ms) + "," + line + "\n";
                        batch_count++;            

                        // Acumula também em buffers por tipo (mesmo formato)
                        if (line.rfind("B:", 0) == 0) {
                            b_batch += ts + "," + std::to_string(reply_length) + "," + std::to_string(delta_ms) + "," + line + "\n";
                        } else if (line.rfind("V:", 0) == 0) {
                            v_batch += ts + "," + std::to_string(reply_length) + "," + std::to_string(delta_ms) + "," + line + "\n";
                        } else if (line.rfind("T:", 0) == 0) {
                            t_batch += ts + "," + std::to_string(reply_length) + "," + std::to_string(delta_ms) + "," + line + "\n";
                        } else if (line.rfind("Z:", 0) == 0) {
                            z_batch += ts + "," + std::to_string(reply_length) + "," + std::to_string(delta_ms) + "," + line + "\n";
                        }

                        if (batch_count >= 10) {
                            if (!raw_file.is_open()) {
                                raw_file.open(raw_filename, std::ios::app);
                                if (!raw_file.is_open()) {
                                    std::cerr << "Erro crítico: arquivo raw está fechado; descartando buffer." << std::endl;
                                }
                            }
                            if (raw_file.is_open()) {
                                raw_file << batch_buffer;
                                raw_file.flush();
                                batch_buffer.clear();
                                batch_count = 0;
                                last_flush = std::chrono::steady_clock::now();
                            }
                            // Flush dos buffers por tipo
                            if (!b_batch.empty()) {
                                if (!b_file.is_open()) b_file.open(b_filename, std::ios::app);
                                if (b_file.is_open()) { b_file << b_batch; b_file.flush(); b_batch.clear(); }
                            }
                            if (!v_batch.empty()) {
                                if (!v_file.is_open()) v_file.open(v_filename, std::ios::app);
                                if (v_file.is_open()) { v_file << v_batch; v_file.flush(); v_batch.clear(); }
                            }
                            if (!t_batch.empty()) {
                                if (!t_file.is_open()) t_file.open(t_filename, std::ios::app);
                                if (t_file.is_open()) { t_file << t_batch; t_file.flush(); t_batch.clear(); }
                            }
                            if (!z_batch.empty()) {
                                if (!z_file.is_open()) z_file.open(z_filename, std::ios::app);
                                if (z_file.is_open()) { z_file << z_batch; z_file.flush(); z_batch.clear(); }
                            }
                        }
                    
                    }

                    if (start_pos > 0) rx_pending.erase(0, start_pos);
                }
            }

            // Saímos do loop de leitura (EOF ou horário). Garantir flush dos buffers pendentes.
            if (!batch_buffer.empty()) {
                if (!raw_file.is_open()) raw_file.open(raw_filename, std::ios::app);
                if (raw_file.is_open()) {
                    raw_file << batch_buffer;
                    raw_file.flush();
                }
                batch_buffer.clear();
                batch_count = 0;
            }
            if (!b_batch.empty()) {
                if (!b_file.is_open()) b_file.open(b_filename, std::ios::app);
                if (b_file.is_open()) { b_file << b_batch; b_file.flush(); }
                b_batch.clear();
            }
            if (!v_batch.empty()) {
                if (!v_file.is_open()) v_file.open(v_filename, std::ios::app);
                if (v_file.is_open()) { v_file << v_batch; v_file.flush(); }
                v_batch.clear();
            }
            if (!t_batch.empty()) {
                if (!t_file.is_open()) t_file.open(t_filename, std::ios::app);
                if (t_file.is_open()) { t_file << t_batch; t_file.flush(); }
                t_batch.clear();
            }
            if (!z_batch.empty()) {
                if (!z_file.is_open()) z_file.open(z_filename, std::ios::app);
                if (z_file.is_open()) { z_file << z_batch; z_file.flush(); }
                z_batch.clear();
            }
            rx_pending.clear();



// Loop de leitura terminou (fim de sessão ou stop). Faz flush do que restou (<10 linhas / <5s).
if (!batch_buffer.empty() && raw_file.is_open()) {
    raw_file << batch_buffer;
    raw_file.flush();
    batch_buffer.clear();
    batch_count = 0;
}
if (!b_batch.empty()) {
    if (!b_file.is_open()) b_file.open(b_filename, std::ios::app);
    if (b_file.is_open()) { b_file << b_batch; b_file.flush(); b_batch.clear(); }
}
if (!v_batch.empty()) {
    if (!v_file.is_open()) v_file.open(v_filename, std::ios::app);
    if (v_file.is_open()) { v_file << v_batch; v_file.flush(); v_batch.clear(); }
}
if (!t_batch.empty()) {
    if (!t_file.is_open()) t_file.open(t_filename, std::ios::app);
    if (t_file.is_open()) { t_file << t_batch; t_file.flush(); t_batch.clear(); }
}
if (!z_batch.empty()) {
    if (!z_file.is_open()) z_file.open(z_filename, std::ios::app);
    if (z_file.is_open()) { z_file << z_batch; z_file.flush(); z_batch.clear(); }
}
rx_pending.clear();

        } catch (const std::exception& e) {
            std::cerr << "Erro de conexão: " << e.what() << std::endl;

            // Não feche o arquivo se estiver aberto; mantenha pronto para reuso
            if (!raw_file.is_open()) {
                raw_file.open(raw_filename, std::ios::app);
                if (!raw_file.is_open()) {
                    std::cerr << "Erro ao manter arquivo raw aberto após exceção: " << raw_filename << std::endl;
                }
            }

            // Flush pendente antes de reconectar
            if (!batch_buffer.empty() && raw_file.is_open()) {
                raw_file << batch_buffer;
                raw_file.flush();
                batch_buffer.clear();
                batch_count = 0;
            }
            if (!b_batch.empty()) {
                if (!b_file.is_open()) b_file.open(b_filename, std::ios::app);
                if (b_file.is_open()) { b_file << b_batch; b_file.flush(); b_batch.clear(); }
            }
            if (!v_batch.empty()) {
                if (!v_file.is_open()) v_file.open(v_filename, std::ios::app);
                if (v_file.is_open()) { v_file << v_batch; v_file.flush(); v_batch.clear(); }
            }
            if (!t_batch.empty()) {
                if (!t_file.is_open()) t_file.open(t_filename, std::ios::app);
                if (t_file.is_open()) { t_file << t_batch; t_file.flush(); t_batch.clear(); }
            }
            if (!z_batch.empty()) {
                if (!z_file.is_open()) z_file.open(z_filename, std::ios::app);
                if (z_file.is_open()) { z_file << z_batch; z_file.flush(); z_batch.clear(); }
            }

            // Sai se estiver fora do horário
            if (is_time_to_stop()) {
                std::cerr << "Fora do horário de operação. Aguardando próximo dia..." << std::endl;
                return;
            }

            if (g_stop.load()) { return; }
            std::cerr << "Tentando reconectar em 5 segundos..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));

            // Garantir que arquivo permaneça aberto para continuar gravando
            if (!raw_file.is_open()) {
                raw_file.open(raw_filename, std::ios::app);
                if (!raw_file.is_open()) {
                    std::cerr << "Erro ao reabrir arquivo raw para reconexão: " << raw_filename << std::endl;
                }
            }
            continue;
        }
    }

    // Fecha o arquivo ao sair
    // Flush pendente antes de fechar
    // Nota: variáveis batch_buffer/batch_count estão fora de escopo aqui;
    // o flush final ocorre no catch e na rotação de data.
    if (raw_file.is_open()) {
        raw_file.close();
    }
    std::cout << "Sessão encerrada. Reiniciando em 5 segundos..." << std::endl;
}
int main(int argc, char** argv) {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    ProgramArgs pargs = parse_args(argc, argv);
    if (pargs.help) {
        print_help(argv[0]);
        return 0;
    }
    if (!pargs.raw_file.empty()) {
        // Modo offline: reconstrói B/V/T/Z a partir do raw_data.txt
        rebuild_from_raw(pargs);
        return 0;
    }
    while (!g_stop.load()) {
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
