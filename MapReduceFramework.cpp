#include "MapReduceFramework.h"
#include <atomic>
#include <vector>
#include <algorithm>
#include <iostream>
#include <semaphore.h>
#include "Barrier.h"
#include <unistd.h>

bool pair_comparator_intermediate(const IntermediatePair& a, const IntermediatePair& b);
class JobContext;
using std::vector;
using std::pair;
using std::cout;
using std::cerr;


using std::endl;

void lock_mutex(pthread_mutex_t* mutex) {
  if (pthread_mutex_lock(mutex)) {
    cout << "system error: " << "failed to lock mutex" << endl;
    exit(EXIT_FAILURE);
  }
}



void unlock_mutex(pthread_mutex_t* mutex) {
  if (pthread_mutex_unlock(mutex)) {
    cout << "system error: " << "failed to unlock mutex" << endl;
    exit(EXIT_FAILURE);
  }
}

class ThreadContext {
  JobContext* _job_context;
  int _threadID;
  const MapReduceClient* _client;
  const InputVec* _inputVec;
  vector<IntermediateVec> _intermediate_vecs;

 public:
  ThreadContext(int id, const MapReduceClient* client, const InputVec* inputVec, JobContext* job_context):
                _job_context(job_context), _threadID(id), _client(client),_inputVec(inputVec){}

  JobContext *GetJobContext() const {
    return _job_context;
  }

  size_t get_input_vec_size() const {
    return _inputVec->size();
  }

  int GetThreadId() const {
    return _threadID;
  }

  void Update_Intermediate_Vecs() {
    IntermediateVec new_vec;
    _intermediate_vecs.push_back(new_vec);
  }

  const MapReduceClient *GetClient() const {
    return _client;
  }
  const InputVec *GetInputVec() const {
    return _inputVec;
  }

  void add_to_intermediate(IntermediatePair pair) {
    _intermediate_vecs.back().push_back(pair);
  }


  const vector<IntermediateVec> &GetIntermediateVecs() const {
    return _intermediate_vecs;
  }
};

class JobContext {
  int _n_threads;
  JobState _job_state;
  std::atomic<uint32_t> _finished_pairs;
  std::atomic<uint32_t> _next_input_idx;
  std::atomic<uint8_t> _stage;
  pthread_mutex_t _mapMtx;
  pthread_mutex_t _reduceMtx;
  pthread_mutex_t _stateMtx;
  pthread_mutex_t _addToOutputMtx;
  Barrier _barrier;
  pthread_t* _threads;
  vector<ThreadContext*> _contexts;
  size_t _cur_stage_input_size;
  OutputVec* _total_output;
  vector<IntermediateVec>* _shuffle_output;
  bool _wait_called;
  bool _close_called;
  pthread_mutex_t _waitMtx;


 public:
  JobContext(int n_threads, size_t input_vec_size,OutputVec* total_output):
  _n_threads(n_threads), _job_state({UNDEFINED_STAGE, 0.0}), _finished_pairs(0), _next_input_idx(0),
  _stage(0), _mapMtx(PTHREAD_MUTEX_INITIALIZER), _reduceMtx(PTHREAD_MUTEX_INITIALIZER),
  _stateMtx (PTHREAD_MUTEX_INITIALIZER), _addToOutputMtx(PTHREAD_MUTEX_INITIALIZER), _waitMtx(PTHREAD_MUTEX_INITIALIZER),
  _barrier (0), _threads(new pthread_t[n_threads]),_cur_stage_input_size(input_vec_size),_total_output(total_output),
  _shuffle_output(new vector<IntermediateVec>()), _wait_called(false), _close_called(false) {} // TODO: free _shuffle_output and _threads

  int GetNThreads() const {
    return _n_threads;
  }

  ~JobContext() {
    for (auto& _context : _contexts) {
      delete _context;
    }
    delete[] _threads;
    delete _shuffle_output;
  }

  void add_to_shuffle_output (const IntermediateVec& vec) const
  {
    _shuffle_output->push_back(vec);
  }

  const vector<IntermediateVec>* get_shuffle_output () const
  {
    return _shuffle_output;
  }

  bool is_close_called () const
  {
    return _close_called;
  }

  void set_close_called (bool close_called)
  {
    _close_called = close_called;
  }

  bool set_wait_called() {
    lock_mutex(&_waitMtx);
    bool ret_val = _wait_called;
    _wait_called = true;
    unlock_mutex(&_waitMtx);
    return ret_val;
  }

  void SetCurStageInputSize(size_t cur_stage_input_size) {
    _cur_stage_input_size = cur_stage_input_size;
  }

  void add_to_total_output(OutputPair pair) {
    lock_mutex (&_addToOutputMtx);
    _total_output->push_back(pair);
    unlock_mutex (&_addToOutputMtx);
  }

  JobState &GetJobState(){
    lock_mutex (&_stateMtx);
    _job_state.stage = static_cast<stage_t>(_stage.load ());
    _job_state.percentage = std::min((float)(_finished_pairs.load()) * ((float)100/(float)_cur_stage_input_size),(float)100);
    unlock_mutex (&_stateMtx);
    return _job_state;
  }

  const pthread_t* GetThreads() const {
    return _threads;
  }

  pthread_t* set_thread_by_id(int id) {
    return &_threads[id];
  }

  ThreadContext* set_context_by_id(int id) {
    return _contexts[id];
  }

  const vector<ThreadContext*> &GetContexts() const {
    return _contexts;
  }

  void add_context(ThreadContext* context) {
    _contexts.push_back(context);
  }

  void SetFinishedPairs() {
    //  if (pthread_mutex_lock(&setFinishedPairsMtx)){exit(EXIT_FAILURE);}
    _finished_pairs += 1;
    //  if (pthread_mutex_unlock(&setFinishedPairsMtx)){exit(EXIT_FAILURE);}
  }

  void SetNextInputIdx(uint32_t count) {
//      if (pthread_mutex_lock(&_reduceMtx)){exit(EXIT_FAILURE);}
    _next_input_idx = count;
//      if (pthread_mutex_unlock(&_reduceMtx)){exit(EXIT_FAILURE);}
  }

  void move_to_next_state() {
    _stage++;
    _finished_pairs = 0;
  }

  int GetNextInputIdxMap(){
    lock_mutex (&_mapMtx);
    int ret_val = (int)_next_input_idx.load();
    _next_input_idx += 1;
    unlock_mutex (&_mapMtx);
    return ret_val;
  }

  int GetNextInputIdxReduce(){
    lock_mutex (&_reduceMtx);
    int ret_val = (int)_next_input_idx.load();
    _next_input_idx -= 1;
    unlock_mutex (&_reduceMtx);
    return ret_val;
  }

  int GetStage()  {
    return (int)_stage.load();
  }

  pthread_mutex_t* get_state_mtx ()
  {
    return &_stateMtx;
  }

  void set_barrier (int multiThreadLevel)
  {
    _barrier.set_num_threads(multiThreadLevel);
  }

  Barrier* get_barrier ()
  {
    return &_barrier;
  }

};

void map_operation(ThreadContext* thread_context) {
  int input_idx;
  while ((input_idx = thread_context->GetJobContext()->GetNextInputIdxMap()) < (int)thread_context->get_input_vec_size()) {
    thread_context->Update_Intermediate_Vecs();
      lock_mutex(thread_context->GetJobContext()->get_state_mtx());
      if (thread_context->GetJobContext()->GetStage() == UNDEFINED_STAGE) {
          thread_context->GetJobContext()->move_to_next_state();
        }
      unlock_mutex(thread_context->GetJobContext()->get_state_mtx());
      auto pair =thread_context->GetInputVec()->at(input_idx);
    thread_context->GetClient()->map(pair.first, pair.second, thread_context);
    thread_context->GetJobContext()->SetFinishedPairs();
  }
}

void shuffle(ThreadContext* thread_context) {
  IntermediateVec sorted;
  for (auto& context : thread_context->GetJobContext()->GetContexts()) {;
      if (context->GetIntermediateVecs().empty()) {
        continue;
      }
      for (auto& vec : context->GetIntermediateVecs()) {
        sorted.insert(sorted.end(), vec.begin(), vec.end());
      }
    }
  size_t count_different = 0;
  if (!sorted.empty()) {
      std::sort(sorted.begin(), sorted.end(), pair_comparator_intermediate);
      count_different++;
      K2* cur_key = sorted.at(0).first;
      for (auto& pair : sorted) {
        if (*cur_key < *pair.first) {
          count_different++;
          cur_key = pair.first;
        }
      }
      thread_context->GetJobContext()->SetCurStageInputSize(count_different);
      pair<K2*, V2*> cur_pair;
      while (!sorted.empty()) {
        IntermediateVec cur_vec;
        cur_pair = sorted.back();
        while (!sorted.empty() && !(*sorted.back().first < *cur_pair.first)) {
          cur_vec.push_back(sorted.back());
          sorted.pop_back();
        }
        thread_context->GetJobContext()->add_to_shuffle_output(cur_vec);
        thread_context->GetJobContext()->SetFinishedPairs();
      }
      thread_context->GetJobContext()->SetNextInputIdx(count_different - 1);
    }
}

void reduce_operation(ThreadContext* thread_context) {
  int input_idx;
    while ((input_idx = thread_context->GetJobContext()->GetNextInputIdxReduce()) >= 0) {
      auto vec = thread_context->GetJobContext()->get_shuffle_output()->at(input_idx);
      thread_context->GetClient()->reduce(&vec, thread_context);
      thread_context->GetJobContext()->SetFinishedPairs();
  }
}

void* thread_operation(void* arg) {

  auto* thread_context = static_cast<ThreadContext*>(arg) ;
  int id = thread_context->GetThreadId();

  //map
  map_operation(thread_context);
  //barrier
  thread_context->GetJobContext()->get_barrier()->barrier();
  //if id==0 and everybody finished map do shuffle
  if (id == 0) {
    //change state to shuffle
    lock_mutex(thread_context->GetJobContext()->get_state_mtx());;
      thread_context->GetJobContext()->move_to_next_state();
      unlock_mutex (thread_context->GetJobContext()->get_state_mtx());
    shuffle(thread_context);
    //change state to reduce
    lock_mutex(thread_context->GetJobContext()->get_state_mtx());
    thread_context->GetJobContext()->move_to_next_state();
    unlock_mutex (thread_context->GetJobContext()->get_state_mtx());
  }

  thread_context->GetJobContext()->get_barrier()->barrier();

  //reduce
  reduce_operation(thread_context);
  return nullptr;
}



JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  auto* job_context = new JobContext(multiThreadLevel, inputVec.size(), &outputVec);
  job_context->set_barrier(multiThreadLevel);

  // create multithreading with function that calls map and reduce
  for (int i = 0; i < multiThreadLevel; ++i) {
    auto* thread_context = new ThreadContext(i, &client, &inputVec, job_context);
    job_context->add_context(thread_context);
  }
  for (int i=0; i < multiThreadLevel; i++) {
    if (pthread_create(job_context->set_thread_by_id(i), nullptr, thread_operation, job_context->set_context_by_id(i)) != 0){
      std::cerr<<"system error: text" << std::endl;
      exit(1);
      }
  }
  return static_cast<JobHandle> (job_context);
}

void emit2 (K2* key, V2* value, void* context) {
  auto* thread_context = static_cast<ThreadContext*>(context);
  thread_context->add_to_intermediate(IntermediatePair(key, value));
}

void emit3 (K3* key, V3* value, void* context) {
  auto* thread_context = static_cast<ThreadContext*>(context);
  thread_context->GetJobContext()->add_to_total_output(OutputPair(key, value));

}

void getJobState(JobHandle job, JobState* state) {
  auto* job_context = static_cast<JobContext*> (job);
  *state = job_context->GetJobState();

}

void waitForJob(JobHandle job) {
  auto* job_context = static_cast<JobContext*> (job);
  if (!job_context->set_wait_called()) {
    for (int i = 0; i < job_context->GetNThreads(); i++) {
      if (pthread_join(job_context->GetThreads()[i], nullptr) != 0) {
        cout << "system error: " << "pthread_join failure" << endl;
        exit(EXIT_FAILURE);
      }
    }
  }
}

bool pair_comparator_intermediate(const IntermediatePair& a, const IntermediatePair& b) {
  return (*a.first < *b.first);
}

void closeJobHandle(JobHandle job) {
  waitForJob(job);
  auto* job_context = static_cast<JobContext*> (job);
  if (!job_context->is_close_called()) {
      job_context->set_close_called(true);
      delete job_context;
    }
}