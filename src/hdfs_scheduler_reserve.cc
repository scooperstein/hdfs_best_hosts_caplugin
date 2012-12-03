#include <iostream>
#include <string>
#include <vector>
// #include <ifstream>
//#include <iostream>
//#include <fstream>
//#include <exception>

#include "hdfs.h"
#include "classad/classad_distribution.h"
#include "classad/classad_stl.h"
#include "classad/fnCall.h"

#include "host.h"

using namespace classad;

static std::string hdfs_scheduler(std::vector<std::string> filepaths);

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


std::string hdfs_scheduler(std::vector<std::string> filepaths) {

 
  //vector<string> filepaths;
  std::vector<host*> uniquehosts;
  std::vector<std::string> bestHostSites;
  int threshold = 0; // method will return sites which host >= threshold of the files locally

  int nTotalHosts = 0;
  //ifstream fileList;
 
  /*fileList.open(argv[1]);
  if (!fileList) {
    cerr<<"could not open input file\n";
  }*/  

  /*string line;
  while (fileList.good()) {
    getline(fileList,line);
    if (line=="") continue;
    filepaths.push_back(line);
    //cout<<"added line : "<<line<<endl;
  }*/

  hdfsFS fs = hdfsConnectAsUserNewInstance("cmshdfs01.hep.wisc.edu",9000,"stephane");
 
  if (fs==NULL) {
    //std::cout<<"Could not connect to hdfs"<<std::endl;
    exit(1); 
  }

  for (int i=0; i<filepaths.size(); i++) {

    //cout<<"looking at filepath : "<<filepaths[i]<<endl;
    if (hdfsExists(fs,filepaths[i].c_str())!=0) {
      //std::cout<<"file: '"<<filepaths[i]<<"' does not exist on filesystem"<<std::endl; 
      //cout<<"i = "<<i<<endl;
      continue;
    }
   
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, filepaths[i].c_str());  
    //if (fileInfo==NULL) std::cout<<"fileInfo is null"<<std::endl;
    
    tOffset length = hdfsGetDefaultBlockSize(fs);
 
    char*** array = hdfsGetHosts(fs,filepaths[i].c_str(),0,length); 
 
    //if (array==NULL) { std::cout<<"array is null"<<std::endl; }
    
    //cout<<"length: :"<<length<<endl; 
    if (*array == 0) continue;

    //cout<<"looking for unique hosts in '"<<filepaths[i]<<"'..."<<endl;
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
          //cout<<"added unique\n";
        }
    
      j++;
    }   
  
    nTotalHosts += j; 

  }

  /*cout<<"Unique hosts: ";
    for (int l=0; l<uniquehosts.size(); l++) {
      cout<<uniquehosts[l]<<", ";
    }*/

    //std::cout<<"\nnumber of unique hosts: "<<uniquehosts.size()<<std::endl;
    //std::cout<<"total number of host spots: "<<nTotalHosts<<std::endl;

  double tallytotal = 0;
  double maxTally = 1;
  for (int k =0; k<uniquehosts.size(); k++) {
    
    if (uniquehosts[k]->tally > maxTally) {
      maxTally = uniquehosts[k]->tally;
    }

    if (uniquehosts[k]->tally >= threshold) {
      bestHostSites.push_back(uniquehosts[k]->name);
    }

  }

  //std::cout<<"max. # blocks hosted by one site: "<<maxTally<<std::endl;
  
  hdfsDisconnect(fs);
  
  return bestHostSites[0];
}

