
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

using namespace graphchi;

int         iterationcount = 0;
bool        scheduler = true;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program. 
 */
typedef vid_t VertexDataType;       // vid_t is the vertex id type
typedef vid_t EdgeDataType;

/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct ConnectedComponentsProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    
    bool converged;
    
    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself). 
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {
        
        gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
        
        if (gcontext.iteration == 0) {
            vertex.set_data(vertex.id());
            gcontext.scheduler->add_task(vertex.id());
        }
        
        /* On subsequent iterations, find the minimum label of my neighbors */
        vid_t curmin = vertex.get_data();
        for(int i=0; i < vertex.num_edges(); i++) {
            vid_t nblabel = vertex.edge(i)->get_data();
            if (gcontext.iteration == 0) nblabel = vertex.edge(i)->vertex_id();  // Note!
            curmin = std::min(nblabel, curmin); 
        }
        
        /* Set my label */
        vertex.set_data(curmin);
        
        /** 
         * Broadcast new label to neighbors by writing the value
         * to the incident edges.
         * Note: on first iteration, write only to out-edges to avoid
         * overwriting data (this is kind of a subtle point)
         */
        vid_t label = vertex.get_data();
        
        if (gcontext.iteration > 0) {
            for(int i=0; i < vertex.num_edges(); i++) {
                if (label < vertex.edge(i)->get_data()) {
                    vertex.edge(i)->set_data(label);
                    /* Schedule neighbor for update */
                    gcontext.scheduler->add_task(vertex.edge(i)->vertex_id(), true);
                    converged = false;
                }
            }
        } else if (gcontext.iteration == 0) {
            for(int i=0; i < vertex.num_outedges(); i++) {
                vertex.outedge(i)->set_data(label);
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



struct ConnectedComponentsInmem : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType * vertex_values;

    vid_t neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return vertex_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, vid_t value)
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
            std::cout << "vertex: " << vertex.id() << std::endl;
            for (int i = 0; i < vertex.num_inedges(); ++ i) {
                std::cout << vertex.inedge(i)->vertexid << " ";
            }
            std::cout << std::endl;
            for (int i = 0; i < vertex.num_outedges(); ++ i) {
                std::cout << vertex.outedge(i)->vertexid << " ";
            }
            std::cout << std::endl;

            set_data(vertex, vertex.id());
            /* Schedule neighbor for update */
            gcontext.scheduler->add_task(vertex.id());
            return;
        }
        else
        {
            vid_t curmin = vertex_values[vertex.id()];
            for(int i=0; i < vertex.num_edges(); i++)
            {
                vid_t nblabel = neighbor_value(vertex.edge(i));
                curmin = std::min(nblabel, curmin);

            }
            if ( curmin < vertex.get_data() )
            {
                for(int i=0; i < vertex.num_edges(); i++)
                {
                    if (curmin < neighbor_value(vertex.edge(i)))
                    {
                        /* Schedule neighbor for update */
                        gcontext.scheduler->add_task(vertex.edge(i)->vertex_id());
                    }
                }
                set_data(vertex, curmin);
            }
        }



        /* On subsequent iterations, find the minimum label of my neighbors */


        /* If my label changes, schedule neighbors */


    }

    void before_iteration(int iteration, graphchi_context &ctx)
    {
        if (iteration == 0)
        {
            /* initialize  each vertex with its own lable */
            vertex_values = new VertexDataType[ctx.nvertices];
            for(int i=0; i < (int)ctx.nvertices; i++)
            {
                vertex_values[i] = i;
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
    metrics m("connected-components");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 1000); // Number of iterations (max)
    //scheduler            = get_option_int("scheduler", false);
    scheduler = true;  // Always run with scheduler
    
    /* Process input file - if not already preprocessed */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));
    
    
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    
    /* Run */
    bool inmemmode = engine.num_vertices() * sizeof(EdgeDataType) < (size_t)engine.get_membudget_mb() * 1024L * 1024L;
    std::cout << "memory messurement:" << std::endl;
    std::cout << engine.num_vertices() * sizeof(EdgeDataType) << " " << (size_t)engine.get_membudget_mb() * 1024L * 1024L << std::endl;
    if (inmemmode) {
		engine.set_modifies_inedges(false); // Improves I/O performance.
		engine.set_modifies_outedges(false); // Improves I/O performance.
		
		ConnectedComponentsInmem program;
		engine.run(program, niters);
		std::cout << "Running ConnectedComponents by holding vertices in-memory mode!" << std::endl;
    }
    else {
	    ConnectedComponentsProgram program;
	    engine.run(program, niters);
	    std::cout << "Running ConnectedComponents with EM mode!" << std::endl;
	}
    
    
    /* Run analysis of the connected components  (output is written to a file) */
    m.start_time("label-analysis");
    
    analyze_labels<vid_t>(filename);
    
    m.stop_time("label-analysis");
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}

