//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/logger/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <thread>
#include <string>
#include <getopt.h>
#include <sys/stat.h>

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/logging/log_manager.h"

#include "backend/benchmark/logger/logger_workload.h"
#include "backend/benchmark/logger/logger_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

namespace peloton {
namespace benchmark {

namespace ycsb {

configuration state;

} // namespace ycsb

namespace logger {

//===--------------------------------------------------------------------===//
// PREPARE LOG FILE
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// 1. Standby -- Bootstrap
// 2. Recovery -- Optional
// 3. Logging -- Collect data and flush when commit
// 4. Terminate -- Collect any remaining data and flush
// 5. Sleep -- Disconnect backend loggers and frontend logger from manager
//===--------------------------------------------------------------------===//

#define LOGGING_TESTS_DATABASE_OID 20000
#define LOGGING_TESTS_TABLE_OID 10000

std::ofstream out("outputfile.summary");

size_t GetLogFileSize();

static void WriteOutput(double value) {
	std::cout << "----------------------------------------------------------\n";
	std::cout << state.logging_type << " " << state.column_count << " "
			<< state.tuple_count << " " << state.backend_count << " "
			<< state.wait_timeout << " :: ";
	std::cout << value << "\n";

	auto &storage_manager = storage::StorageManager::GetInstance();
	auto& log_manager = logging::LogManager::GetInstance();
	auto frontend_logger = log_manager.GetFrontendLogger();
	auto fsync_count = 0;
	if(frontend_logger != nullptr){
		fsync_count = frontend_logger->GetFsyncCount();
	}

	std::cout << "fsync count : " << fsync_count << " ";
	std::cout << "clflush count : " << storage_manager.GetClflushCount() << " ";
	std::cout << "msync count : " << storage_manager.GetMsyncCount() << "\n";

	out << state.logging_type << " ";
	out << state.column_count << " ";
	out << state.tuple_count << " ";
	out << state.backend_count << " ";
	out << state.wait_timeout << " ";
	out << value << "\n";
	out.flush();
}

std::string GetFilePath(std::string directory_path, std::string file_name) {
	std::string file_path = directory_path;

	// Add a trailing slash to a file path if needed
	if (!file_path.empty() && file_path.back() != '/') file_path += '/';

	file_path += file_name;

	// std::cout << "File Path :: " << file_path << "\n";

	return file_path;
}

/**
 * @brief writing a simple log file
 */
bool PrepareLogFile(std::string file_name) {
	auto file_path = GetFilePath(state.log_file_dir, file_name);

	std::ifstream log_file(file_path);

	// Reset the log file if exists
	if (log_file.good()) {
		std::remove(file_path.c_str());
	}
	log_file.close();

	// start a thread for logging
	auto& log_manager = logging::LogManager::GetInstance();

	if (log_manager.ActiveFrontendLoggerCount() > 0) {
		LOG_ERROR("another logging thread is running now");
		return false;
	}

	// INVALID MODE
	if (peloton_logging_mode == LOGGING_TYPE_INVALID) {
		BuildLog();
		return true;
	}

	// TODO:
	std::cout << "Log path :: " << file_path << "\n";

	// set log file and logging type
	log_manager.SetLogFileName(file_path);

	// start off the frontend logger of appropriate type in STANDBY mode
	std::thread thread(&logging::LogManager::StartStandbyMode, &log_manager);

	// wait for the frontend logger to enter STANDBY mode
	log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, true);

	// STANDBY -> RECOVERY mode
	log_manager.StartRecoveryMode();

	// Wait for the frontend logger to enter LOGGING mode
	log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING, true);

	// Build the log
	BuildLog();

	//  Wait for the mode transition :: LOGGING -> TERMINATE -> SLEEP
	if (log_manager.EndLogging()) {
		thread.join();
		return true;
	}

	LOG_ERROR("Failed to terminate logging thread");
	return false;
}

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void ResetSystem() {
	// XXX Initialize oid since we assume that we restart the system

	auto& txn_manager = concurrency::TransactionManager::GetInstance();
	txn_manager.ResetStates();
}

/**
 * @brief recover the database and check the tuples
 */
void DoRecovery(std::string file_name) {
	auto file_path = GetFilePath(state.log_file_dir, file_name);

	std::ifstream log_file(file_path);

	// Reset the log file if exists
	log_file.close();

	ycsb::ParseArguments(0, nullptr, ycsb::state);

	ycsb::CreateYCSBDatabase();

	// start a thread for logging
	auto& log_manager = logging::LogManager::GetInstance();
	if (log_manager.ActiveFrontendLoggerCount() > 0) {
		LOG_ERROR("another logging thread is running now");
		return;
	}

	std::cout << "YCSB Table " << ycsb::user_table->GetTileGroupCount();

	//===--------------------------------------------------------------------===//
	// RECOVERY
	//===--------------------------------------------------------------------===//

	Timer<std::milli> timer;
	timer.Start();

	// set log file and logging type
	log_manager.SetLogFileName(file_path);

	// start off the frontend logger of appropriate type in STANDBY mode
	std::thread thread(&logging::LogManager::StartStandbyMode, &log_manager);

	// wait for the frontend logger to enter STANDBY mode
	log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, true);

	// STANDBY -> RECOVERY mode
	log_manager.StartRecoveryMode();

	// Wait for the frontend logger to enter LOGGING mode after recovery
	log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING, true);

	timer.Stop();

	// Recovery time
	if (state.experiment_type == EXPERIMENT_TYPE_RECOVERY)
		WriteOutput(timer.GetDuration());

	if (log_manager.EndLogging()) {
		thread.join();
	} else {
		LOG_ERROR("Failed to terminate logging thread");
	}

	std::cout << "YCSB Table " << ycsb::user_table->GetTileGroupCount();

}

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void BuildLog() {

	ycsb::ParseArguments(0, nullptr, ycsb::state);

	ycsb::CreateYCSBDatabase();

	ycsb::LoadYCSBDatabase();

	//===--------------------------------------------------------------------===//
	// ACTIVE PROCESSING
	//===--------------------------------------------------------------------===//
	Timer<std::milli> timer;
	timer.Start();

	ycsb::RunWorkload();

	timer.Stop();

	// Build log time
	if (state.experiment_type == EXPERIMENT_TYPE_ACTIVE ||
			state.experiment_type == EXPERIMENT_TYPE_WAIT) {
		WriteOutput(timer.GetDuration());
	} else if (state.experiment_type == EXPERIMENT_TYPE_STORAGE) {
		auto log_file_size = GetLogFileSize();
		std::cout << "Log file size :: " << log_file_size << "\n";
		WriteOutput(log_file_size);
	}

}

size_t GetLogFileSize() {
	struct stat log_stats;

	auto& log_manager = logging::LogManager::GetInstance();
	std::string log_file_name = log_manager.GetLogFileName();

	// open log file and file descriptor
	// we open it in append + binary mode
	auto log_file = fopen(log_file_name.c_str(), "r");
	if (log_file == NULL) {
		LOG_ERROR("LogFile is NULL");
	}

	// also, get the descriptor
	auto log_file_fd = fileno(log_file);
	if (log_file_fd == -1) {
		LOG_ERROR("log_file_fd is -1");
	}

	fstat(log_file_fd, &log_stats);
	auto log_file_size = log_stats.st_size;

	return log_file_size;
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
