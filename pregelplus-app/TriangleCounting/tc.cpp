#include "basic/pregel-dev.h"
#include "utils/type.h"

#include <algorithm>
#include <iostream>

using namespace std;


struct vertexValue
{
	int count;
	vector<int> edges;
};

ibinstream & operator<<(ibinstream & m, const vertexValue & v)
{
	m << v.count;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, vertexValue & v)
{
	m >> v.count;
	m >> v.edges;
	return m;
}

//====================================

class tcVertex: public Vertex<VertexID, vertexValue, int>
{
	public:
		virtual void compute(MessageContainer & messages)
		{	
			if (step_num() == 1)
			{
				vector<int>& edges = value().edges;
				
				for (int i = 0; i < edges.size(); i++)
				{
					for (int j = i+1; j < edges.size(); j++)
						send_message(edges[i], edges[j]);
				}
				vote_to_halt();
			}
			else
			{
				vector<int>& edges = value().edges;
				sort(edges.begin(), edges.end());
				for (int i = 0; i < messages.size(); i++)
				{
					bool found = binary_search(edges.begin(), edges.end(), messages[i]);
					if (found) value().count++;	
				}
				
				vote_to_halt();
			}
		}
};

class tcWorker: public Worker<tcVertex>
{
	char buf[100];

public:
	//C version
	virtual tcVertex* toVertex(char* line)
	{
		char * pch;
		pch = strtok(line, "\t");
		tcVertex* v = new tcVertex;
		v->id = atoi(pch);
		pch = strtok(NULL, " ");
		int num = atoi(pch);
		v->value().count = 0; 
		for (int i = 0; i < num; i++)
		{
			pch = strtok(NULL, " ");
			int vid = atoi(pch);
			v->value().edges.push_back(vid);
		}
		return v;
	}

	virtual void toline(tcVertex* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d\t%d\n", v->id, v->value().count);
		writer.write(buf);
	}
};

void pregel_tc(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	tcWorker worker;
	worker.run(param);
}

int main(int argc, char* argv[]){
	init_workers();
	pregel_tc(argv[1], argv[2]);
	worker_finalize();
	return 0;
}

