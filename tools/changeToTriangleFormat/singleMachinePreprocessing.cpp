#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
using namespace std;
vector<vector<int> > adj;
vector<pair<int, int> > edge;
vector<int> newEdge;
bool cmp(const pair<int,int>& p1, const pair<int, int>& p2)
{
    if (p1.first == p2.first) return p1.second < p2.second;
    else return p1.first < p2.first;
    
}
int main(int argc, char* argv[])
{
    fstream in;
    in.open(argv[1], ios::in);
    fstream out;
    out.open(argv[2], ios::out);

    int src, num, dst;
    // load graph
    while(in >> src) {
        adj.resize(src+1);
        in >> num;
        adj[src].resize(num);
        for (int i = 0; i < num; ++ i) in >> adj[src][i];
    }
    in.close();
	cout << "load done" << endl;

    // process
    for (int i = 0; i < adj.size(); ++ i) {
        newEdge.clear();
        edge.resize(adj[i].size());
        for (int j = 0; j < adj[i].size(); ++ j) {
            edge[j] = make_pair(adj[adj[i][j]].size(), adj[i][j]);
        }
        sort(edge.begin(), edge.end(), cmp);
        pair<int, int> ip(adj[i].size(), i);
        for (int j = 0; j < edge.size(); ++ j) {
            if (cmp(ip, edge[j]) == 1) 
                newEdge.push_back(edge[j].second);
        }
        out << i << "\t" << newEdge.size() << " ";
        for (int j = 0; j < newEdge.size(); ++ j) {
            out << newEdge[j] << " ";
        }
        out << endl;
    }
    out.close();
}
