#include <iostream>
#include <string>
#include <vector>

#include "hdfs.h"
#include "classad/classad_distribution.h"
#include "classad/classad_stl.h"
#include "classad/fnCall.h"

#include "host.h"

using namespace classad;

static bool hdfs_best_hosts(const char *name, ArgumentList const &arguments,
    EvalState &state, Value &result);
std::vector<host*> get_top_n(std::vector<host*> hosts, int n);

static ClassAdFunctionMapping functions[] =
{
    { "hdfs_best_hosts", (void *) hdfs_best_hosts, 0 },
    { "hdfs_best_hosts", (void *) hdfs_best_hosts, 0 },
    { "",            NULL,                 0 }
};


/***************************************************************************
 * 
 * Required entry point for the library.  This should be the only symbol
 * exported
 *
 ***************************************************************************/
extern "C"
{
        ClassAdFunctionMapping *Init(void)
        {
                return functions;
        }
}


static bool hdfs_best_hosts(const char *, // name
                           const ArgumentList &arguments,
                           EvalState & state,
                           Value &result)
{

  std::vector<std::string> filepaths;
  std::vector<host*> uniquehosts;  
  std::vector<host*> top_n_hosts;
  std::vector<ExprTree*> best_hosts; 
  int n;

  if (arguments.size()!=2) {
    result.SetErrorValue();
    CondorErrMsg = "Usage: hdfs_best_hosts[filename or list of filenames,# host sites returned]";
    return false;
  }
  
  ExprList* list;
  std::string filename;
  Value v_1;
  arguments[0]->Evaluate(state,v_1);

  if (!v_1.IsStringValue(filename) && !v_1.IsListValue(list)) {
    result.SetErrorValue();
    CondorErrMsg = "first argument should either be a filepath or a list of filepaths";
    return false;
  }

  Value v_2;
  arguments[1]->Evaluate(state,v_2);
  if (!v_2.IsIntegerValue(n)) {
    result.SetErrorValue();
    CondorErrMsg = "final argument should be the number of best host sites returned";
    return false;
  }

  ExprList &files_requested = *list;
  if (v_1.IsStringValue(filename)) {
    filepaths.push_back(filename);
  }

  else {
    for (ExprList::const_iterator it = files_requested.begin(); it != files_requested.end(); ++it) {
      Value filename_arg;
      const ExprTree * tree = *it;
  
      if (!tree || !tree->Evaluate(state, filename_arg)) {
        
        result.SetErrorValue();
        CondorErrMsg = "Could not evaluate filename";
        return false;
      }     
      std::string single_filename;
      if (!filename_arg.IsStringValue(single_filename)) {
        CondorErrMsg = "filename is not string value";
        return false;
      }

      size_t pos = single_filename.find("file:/hdfs/");
      if (pos != std::string::npos) {
          // remove 'file:/hdfs' from filename'
          single_filename.erase(pos, 10);
      }
      size_t pos2 = single_filename.find("//");
      if (pos2 != std::string::npos) {
          single_filename.erase(pos,1);
      } 
      filepaths.push_back(single_filename);
    }
    
  }

  hdfsFS fs = hdfsConnect("default",0);

  if (fs==NULL) {
    CondorErrMsg = "Could not connect to hdfs";
    result.SetErrorValue();
    return false; 
  }

  for (int i=0; i<filepaths.size(); i++) {

    if (hdfsExists(fs,filepaths[i].c_str())!=0) {
      CondorErrMsg = std::string("File ") + filepaths[i] + std::string("does not exist on filesystem.");
      result.SetErrorValue();
      return false;
    }
   
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, filepaths[i].c_str());  
    tOffset length = hdfsGetDefaultBlockSize(fs); 
    char*** array = hdfsGetHosts(fs,filepaths[i].c_str(),0,length); 
     
    if (*array == 0) continue;

    bool done = false;
    int j = 0;
    while (!done) {
   
        if (array[0][j]==NULL) { done = true; continue; }  
        
        bool isUnique = true;
        for (int k=0; k<uniquehosts.size(); k++) {
   
          if (uniquehosts[k]->name.compare(array[0][j]) == 0) {
            isUnique=false;
            uniquehosts[k]->addSite();
          }
         
        }
        if (isUnique) {
          uniquehosts.push_back(new host(std::string(array[0][j])));
        }
    
      j++;
    }    

  }

  // create return value
  top_n_hosts = get_top_n(uniquehosts, n); 
  std::string s;
  for (int k =0; k<top_n_hosts.size(); k++) {
    Value v;
    v.SetStringValue(top_n_hosts[k]->name);
    best_hosts.push_back(Literal::MakeLiteral(v));
    // for parseable string form
    s += top_n_hosts[k]->name + ',';
  }

  hdfsDisconnect(fs);
  ExprList *l = new ExprList(best_hosts);
  //result.SetListValue(l); 

  // return list of best hosts in parseable string form
  result.SetStringValue(s);    
  return true;
}

// a modified selection sort to get the n hosts with the greatest # files hosted locally
std::vector<host*> get_top_n(std::vector<host*> hosts, int n) {
  std::vector<host*> best_hosts;
  int maxTally; 
  int maxIndex;

  for (int i=0;i<n;i++) {
    maxTally = hosts[0]->tally;
    maxIndex = 0;

    if (hosts.size()==0) break; 

    for(int k=1;k<hosts.size();k++) {
      if (hosts[k]->tally > maxTally) {
        maxTally = hosts[k]->tally;
        maxIndex = k;
      }
    }

    best_hosts.push_back(hosts[maxIndex]);
    hosts.erase(hosts.begin()+maxIndex);
  }
  return best_hosts;
}

