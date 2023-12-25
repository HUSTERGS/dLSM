#include "db/dbformat.h"
#include <iostream>
#include <memory_node/memory_node_keeper.h>
#include <csignal>
#include "util/rdma.h"


volatile sig_atomic_t gSignalStatus = 0;

// Signal 处理函数
void signalHandler(int signal) {
  std::cout << "Ctrl+C received." << std::endl;
  for (int i = 0; i < config::kNumLevels; i++) {
    if (dLSM::duration_recorder[i]) {
      delete dLSM::duration_recorder[i];
    }
  }
  exit(0);
}


//namespace dLSM{
int main(int argc,char* argv[])
{
  std::signal(SIGINT, signalHandler);
  dLSM::Memory_Node_Keeper* mn_keeper;
  if (argc == 4){
    uint32_t tcp_port;
    int pr_size;
    int Memory_server_id;
    char* value = argv[1];
    std::stringstream strValue1;
    strValue1 << value;
    strValue1 >> tcp_port;
    value = argv[2];
    std::stringstream strValue2;
    //  strValue.str("");
    strValue2 << value;
    strValue2 >> pr_size;
    value = argv[3];
    std::stringstream strValue3;
    //  strValue.str("");
    strValue3 << value;
    strValue3 >> Memory_server_id;
     mn_keeper = new dLSM::Memory_Node_Keeper(true, tcp_port, pr_size);
     dLSM::RDMA_Manager::node_id = 2* Memory_server_id;
  }else{
    mn_keeper = new dLSM::Memory_Node_Keeper(true, 19843, 88);
    dLSM::RDMA_Manager::node_id = 0;
  }
  for (int i = 0; i < config::kNumLevels; i++) {
    dLSM::duration_recorder[i] = new RecordDuration(RecordDuration::MNCompactionDuration, i);
  }

  mn_keeper->SetBackgroundThreads(12, dLSM::ThreadPoolType::CompactionThreadPool);
  mn_keeper->Server_to_Client_Communication();
  delete mn_keeper;

  return 0;
}