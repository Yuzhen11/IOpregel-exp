
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * Application for computing the connected components of a graph.
 * The algorithm is simple: on first iteration each vertex sends its
 * id to neighboring vertices. On subsequent iterations, each vertex chooses
 * the smallest id of its neighbors and broadcasts its (new) label to
 * its neighbors. The algorithm terminates when no vertex changes label.
 *
 * @section REMARKS
 *
 * This application is interesting demonstration of the asyncronous capabilities
 * of GraphChi, improving the convergence considerably. Consider
 * a chain graph 0->1->2->...->n. First, vertex 0 will write its value to its edges,
 * which will be observed by vertex 1 immediatelly, changing its label to 0. Nexgt,
 * vertex 2 changes its value to 0, and so on. This all happens in one iteration.
 * A subtle issue is that as any pair of vertices a<->b share an edge, they will
 * overwrite each others value. However, because they will be never run in parallel
 * (due to deterministic parallellism of graphchi), this does not compromise correctness.
 *
 * @author Aapo Kyrola
 */


#include <cmath>
#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

int         iterationcount = 0;
bool        scheduler = true;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program. 
 */
typedef int VertexDataType;       // vid_t is the vertex id type
typedef int EdgeDataType;

int SourceVertexId = 0;
const int inf = 1000000000;
/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct UnWeightedSSSPProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    
    bool converged;
    
    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself). 
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {
        
        //gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
        
        if (gcontext.iteration == 0) {
        	
        	if (vertex.id() == SourceVertexId)
        	{
        		vertex.set_data(0);
        		//gcontext.scheduler->add_task(vertex.id());
        		for(int i=0; i < vertex.num_outedges(); i++) {
                	vertex.outedge(i)->set_data(1);
                	// Schedule neighbors for update
                	gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id(), true);
            	}
        	}
        	else 
        	{
        		vertex.set_data(inf);
        		for(int i=0; i < vertex.num_outedges(); i++) {
                	vertex.outedge(i)->set_data(inf);
            	}
        	}
        }
        else
        {
        	int curmin = vertex.get_data();
        	for (int i = 0; i < vertex.num_inedges(); i++) {
        		curmin = std::min(curmin, vertex.inedge(i)->get_data());
        	}
        	if (curmin < vertex.get_data())
        	{
        		vertex.set_data(curmin);
        		for (int i = 0; i < vertex.num_outedges(); i++)
        		{
        			vertex.outedge(i)->set_data(curmin+1);
                	// Schedule neighbors for update
                	gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id(), true);
                	converged = false;
        		}
        	}
        	
        }
    }    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &info) {
        iterationcount++;
        converged = iteration > 0;
    }
    
    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &ginfo) {
        if (converged) {
            std::cout << "Converged!" << std::endl;
            ginfo.set_last_iteration(iteration);
        }
    }
    
    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &ginfo) {        
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &ginfo) {        
    }
    
};



struct UnWeightedSSSPInmem : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType * vertex_values;

    int neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return vertex_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, int value)
    {
        vertex_values[vertex.id()] = value;
        vertex.set_data(value);
    }

    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself).
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext)
    {
        /* This program requires selective scheduling. */
        assert(gcontext.scheduler != NULL);

        if(gcontext.iteration == 0)
        {
            if( vertex.id() == SourceVertexId)
            {
                set_data(vertex, 0);
                for(int i=0; i < vertex.num_outedges(); i++)
                {
                	gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                }
            }
            else
                set_data(vertex, inf);

            /* Schedule neighbor for update */

            return;
        }
        else
        {
            /* On subsequent iterations, find the minimum label of my neighbors */
            int curmin = vertex_values[vertex.id()];
            for(int i=0; i < vertex.num_inedges(); i++)
            {
                int ndistance = neighbor_value(vertex.inedge(i)) + 1;
                curmin = std::min(ndistance, curmin);
            }

            /* If my label changes, schedule neighbors */
            if (curmin < vertex.get_data())
            {
                for(int i=0; i < vertex.num_outedges(); i++)
                {
                    int newdistance = curmin + 1;
                    if (newdistance < neighbor_value(vertex.outedge(i)))
                    {
                        /* Schedule neighbor for update */
                        gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    }
                }
            }
            set_data(vertex, curmin);
        }



    }

    void before_iteration(int iteration, graphchi_context &ctx)
    {
        if (iteration == 0)
        {
            /* initialize  each vertex with its own lable */
            vertex_values = new VertexDataType[ctx.nvertices];
            for(int i=0; i < (int)ctx.nvertices; i++)
            {
                if( i == SourceVertexId)
                    vertex_values[i] = 0;
                else
                    vertex_values[i] = inf;

            }
        }
    }

}
;



int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line 
     arguments and the configuration file. */
    graphchi_init(argc, argv);

    /* Metrics object for keeping track of performance counters
     and other information. Currently required. */
    metrics m("UnWeightedSSSP");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 1000); // Number of iterations (max)
    SourceVertexId = get_option_int("src", 0);
    std::cout << "src:   " << SourceVertexId << std::endl;

    //scheduler            = get_option_int("scheduler", false);
    scheduler = true;  // Always run with scheduler
    
    /* Process input file - if not already preprocessed */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));
    
    
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    
    /* Run */
    bool inmemmode = engine.num_vertices() * sizeof(EdgeDataType) < (size_t)engine.get_membudget_mb() * 1024L * 1024L;
    std::cout << "memory messurement:" << std::endl;
    std::cout << engine.num_vertices() * sizeof(EdgeDataType) << " " << (size_t)engine.get_membudget_mb() * 1024L * 1024L << std::endl;
    //inmemmode = 0;
    if (inmemmode) {
		engine.set_modifies_inedges(false); // Improves I/O performance.
		engine.set_modifies_outedges(false); // Improves I/O performance.
		
		UnWeightedSSSPInmem program;
		engine.run(program, niters);
		std::cout << "Running UnweightedSSSP by holding vertices in-memory mode!" << std::endl;
    }
    else {
	    UnWeightedSSSPProgram program;
	    engine.run(program, niters);
	    std::cout << "Running UnweightedSSSP with EM mode!" << std::endl;
	}
	
	
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}

