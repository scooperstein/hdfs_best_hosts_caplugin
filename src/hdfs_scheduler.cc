#include <iostream>
#include <string>
#include <vector>

#include "hdfs.h"
#include "classad/classad_distribution.h"
#include "classad/classad_stl.h"
#include "classad/fnCall.h"

#include "host.h"

using namespace classad;

static bool hdfs_scheduler(const char *name, ArgumentList const &arguments,
    EvalState &state, Value &result);

static ClassAdFunctionMapping functions[] =
{
    { "hdfs_scheduler", (void *) hdfs_scheduler, 0 },
    { "hdfs_scheduler", (void *) hdfs_scheduler, 0 },
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


static bool hdfs_scheduler(const char *, // name
                           const ArgumentList &arguments,
                           EvalState & state,
                           Value &result)
{ 

  std::vector<std::string> filepaths;
  std::vector<host*> uniquehosts;  
  std::vector<ExprTree*> best_hosts; 

  //int threshold = 1; // method will return sites which host >= threshold of the files locally
  
  for (int i=0; i<arguments.size(); i++) {
    Value filenames_arg;

    if (!arguments[i]->Evaluate(state, filenames_arg)) {
      result.SetErrorValue();
      CondorErrMsg = "Could not evaluate filename";
      return false;
    }     
    
    std::string single_filename;
    if (filenames_arg.IsStringValue(single_filename)) {
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
      CondorErrMsg = "File does not exist on filesystem.";
      result.SetErrorValue();
      return false;
    }
   
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, filepaths[i].c_str());  
    if (fileInfo==NULL) {
      CondorErrMsg = "fileInfo is null";
      result.SetErrorValue();
      return false;
    }
    
    tOffset length = hdfsGetDefaultBlockSize(fs);
 
    char*** array = hdfsGetHosts(fs,filepaths[i].c_str(),0,length); 
 
    if (array==NULL) { 
      CondorErrMsg = "array is null";
      result.SetErrorValue();
      return false;
    }
     
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
  
  for (int k =0; k<uniquehosts.size(); k++) {

    if (uniquehosts[k]->tally >= threshold) {
      Value v;
      v.SetStringValue(uniquehosts[k]->name);
      best_hosts.push_back(Literal::MakeLiteral(v));
    }

  }

  
  hdfsDisconnect(fs);
  fclose(fp);
  ExprList *l = new ExprList(best_hosts);
  result.SetListValue(l); 
  return true;
}

std::vector<host*> get_top_n(std:vector<host*> hosts, int n) {
  std::vector<host*> best_hosts;
  host *minHost;
  int minTally;
  for (int i=0,i<hosts.size(),i++) {
    int maxTally;
    int maxIndex;
    if (hosts[i]->tally > minTally) {
      
    }
    if (i<n) {
      best_hosts.push_back(hosts[i]);
    }  

  }

}

