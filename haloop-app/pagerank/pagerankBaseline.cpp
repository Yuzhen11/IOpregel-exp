#include <iostream>
#include <vector>
#include <fstream>
using namespace std;
vector<vector<int> > adj;
int main()
{
	fstream in;
	in.open("toy.txt",ios::in);
	int src;
	int num; int tmp;
	while(in >> src)
	{
		adj.resize(src+1);
		in >> num;
		for (int i = 0; i < num; ++ i) 
		{
			in >> tmp;
			adj[src].push_back(tmp);
		}
	}
	in.close();

	vector<float> pagerank(adj.size());
	vector<float> nextPagerank(adj.size());

	for (int i = 0; i < adj.size(); ++ i){
		pagerank[i] = 1;
		nextPagerank[i] = 0;
	}
	int iteration = 4;
	int iter = 1;
	while(iter <= iteration)
	{
		for (int i = 0; i < adj.size(); ++ i)
		{
			for (int j = 0; j < adj[i].size(); ++ j)
			{
				nextPagerank[adj[i][j]] += pagerank[i]/adj[i].size();	
			}
		}
		cout << "iteration: " << iter << endl;
		for (int i = 0; i < adj.size(); ++ i)
		{
			pagerank[i] = nextPagerank[i]*0.85+0.15;	
			nextPagerank[i] = 0;
			cout << "i = " << i << " pr =" << pagerank[i] << endl;
		}
		iter++;
	}
}
