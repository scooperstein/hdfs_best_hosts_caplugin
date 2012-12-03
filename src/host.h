#ifndef HOST_H
#define HOST_H

#include <string>

class host {

public:
  
  host(std::string s) {
    name = s;
    tally = 1;
  }
  ~host();

  std::string name;
  int tally;
  void addSite() {
    tally++;
  }
  




};


#endif
