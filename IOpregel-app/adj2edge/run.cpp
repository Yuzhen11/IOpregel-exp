#include "ioser.h"
#include <fstream>
#include <sstream>
#include <string>
using namespace std;

int main(int argc, char* argv[])
{
	ifstream in(argv[1]);
	ofbinstream out;
	out.open(argv[2]);
	int a, tmp, deg;
	while(in >> a)
	{
		in >> deg;
		for(int i=0; i<deg; i++){
			in >> tmp;
			out << a << tmp;
		}
	}
	out.close();
	in.close();
    return 0;
}
