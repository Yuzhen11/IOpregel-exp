#include "basic/pregel-dev.h"
#include <float.h>
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//edge lengths are assumed to be 1

//output line format: v \t shortest_path_length  previous_vertex_on_shorest_path
//previous_vertex_on_shorest_path=-1 for source vertex

//#define WEIGHTED_GRAPH

int src = 0;

struct SPEdge_pregel
{
	double len;
	int nb;
};

ibinstream & operator<<(ibinstream & m, const SPEdge_pregel & v){
	m<<v.len;
	m<<v.nb;
	return m;
}

obinstream & operator>>(obinstream & m, SPEdge_pregel & v){
	m>>v.len;
	m>>v.nb;
	return m;
}

//====================================

struct SPValue_pregel
{
	double dist;
	vector<SPEdge_pregel> edges;
};

ibinstream & operator<<(ibinstream & m, const SPValue_pregel & v){
	m<<v.dist;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SPValue_pregel & v){
	m>>v.dist;
	m>>v.edges;
	return m;
}

//====================================

class SPVertex_pregel:public Vertex<VertexID, SPValue_pregel, double>
{
	public:
		void broadcast()
		{
			vector<SPEdge_pregel> & nbs=value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i].nb, value().dist+nbs[i].len);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(id==src)
				{
					value().dist=0;
					broadcast();
				}
				else
				{
					value().dist=DBL_MAX;
				}
			}
			else
			{
				double min=DBL_MAX;
				for(int i=0; i<messages.size(); i++)
				{
                    if (messages[i] < min) min = messages[i];
				}
				if(min < value().dist)
				{
					value().dist=min;
					broadcast();
				}
			}
			vote_to_halt();
		}
};

class SPWorker_pregel:public Worker<SPVertex_pregel>
{
	char buf[1000];

	public:
#ifndef WEIGHTED_GRAPH
		//input line:
		virtual SPVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			SPVertex_pregel* v=new SPVertex_pregel;
			int id=atoi(pch);
			v->id=id;
			if(id==src) v->value().dist=0;
			else
			{
				v->value().dist=DBL_MAX;
				v->vote_to_halt();
			}
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			while(pch=strtok(NULL, " "))
			{
				int nb=atoi(pch);
				double len=1;
				SPEdge_pregel edge={len, nb};
				v->value().edges.push_back(edge);
			}
			return v;
		}
#else
        //weighted graph
		virtual SPVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			SPVertex_pregel* v=new SPVertex_pregel;
			int id=atoi(pch);
			v->id=id;
			if(id==src) v->value().dist=0;
			else
			{
				v->value().dist=DBL_MAX;
				v->vote_to_halt();
			}
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			while(pch=strtok(NULL, " "))
			{
				int nb=atoi(pch);
                pch=strtok(NULL, " ");
				double len = atof(pch);
				SPEdge_pregel edge={len, nb};
				v->value().edges.push_back(edge);
			}
			return v;
		}
#endif

		//output line:
		//vid \t dist from
		virtual void toline(SPVertex_pregel* v, BufferedWriter & writer)
		{
			if(v->value().dist!=DBL_MAX) sprintf(buf, "%d\t%lf\n", v->id, v->value().dist);
			else sprintf(buf, "%d\tunreachable\n", v->id);
			writer.write(buf);
		}
};

class SPCombiner_pregel:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			if(old > new_msg) old=new_msg;
		}
};

void pregel_sssp(int srcID, string in_path, string out_path, bool use_combiner){
	src=srcID;//set the src first

	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	SPWorker_pregel worker;
	SPCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
int main(int argc, char* argv[]){
	init_workers();
	pregel_sssp(atoi(argv[3]), argv[1], argv[2], true);
	worker_finalize();
	return 0;
}
