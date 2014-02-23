#include <graphlab.hpp>
#include <sstream>
#include <string>
#include <fstream>
#include <vector>
#include <stdio.h>
#include <iostream>
using namespace std;

vector<string> productId;
vector<string> userId;
map<string, vector<string> > mapReduceSet;
/**
 * Explode from StackOverflow. http://stackoverflow.com/questions/236129/splitting-a-string-in-c
 */
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
   std::stringstream ss(s);
   std::string item;
   while (std::getline(ss, item, delim)) {
       elems.push_back(item);
   }
   return elems;
}


std::vector<std::string> split(const std::string &s, char delim) {
   std::vector<std::string> elems;
   split(s, delim, elems);
   return elems;
}

void readLine(string line)
{
	vector<string> splitLine=split(line, ' ');	
	if(splitLine.at(0)=="product/productId:")
	{
		productId.push_back(splitLine.at(1));
		cout<<productId.size()<<" "<<productId.at(productId.size()-1)<<endl;
	} 
	else if(splitLine.at(0) == "review/userId:")
	{
		userId.push_back(splitLine.at(1));
	}
}
void readSmallFile(string fileName)
{
	ifstream infile;
	infile.open(fileName.c_str());
	string readString="";
	
	if(infile.is_open())
	{
		while(!infile.eof())
		{
			getline(infile, readString);
			{
				readLine( readString);// rename!		
			}
		}
		infile.close();
	}
}
/* before this function the arrays are 1-1 containing a pid and a userId for each subscript x
 * in this function we must provide the a map of Pid -> vector<userId>
 */
void mapReduce()
{
	vector<int> tempProdId;
	for(unsigned int i=0; i<productId.size();i++)
	{
		{//not found
			vector<string>* tempVec= new vector<string>;
			try
			{
				mapReduceSet.at(productId.at(i)).push_back(userId.at(i));
			}
			catch(exception e)
			{
				tempVec->push_back(userId.at(i));
				mapReduceSet[productId.at(i)]= *tempVec;
			}	
		}
	}
		
}
void outputSet()
{
	for(map<string, vector<string> >::const_iterator ptr=mapReduceSet.begin();ptr!=mapReduceSet.end();ptr++)
	{
		cout<<ptr->first<<"PRODUCT_ID: ";
		for(vector<string>::const_iterator eptr=ptr->second.begin();eptr!=ptr->second.end();eptr++)
		{
			cout<<*eptr<<" ";
		}
		cout<<endl;
	}	
}

int main(int argc, char** argv) {

graphlab::mpi_tools::init(argc, argv);
graphlab::distributed_control dc;
        readSmallFile("/home/cloudera/Downloads/sampleDataReviews.txt");
for(unsigned int j=0; j<productId.size();j++)
{
	cout<<productId.at(j)<<endl;
}
//output();
mapReduce();
cout<<"DIVISION BETWEEN MAPREDUCE"<<endl;
for(map<string, vector<string> >::const_iterator ptr=mapReduceSet.begin();ptr!=mapReduceSet.end();ptr++)
        {
               dc.cout()<<ptr->first<<endl;
                for(vector<string>::const_iterator eptr=ptr->second.begin();eptr!=ptr->second.end();eptr++)
                {
                        dc.cout()<<*eptr<<endl;
                }
                dc.cout()<<endl;
        }


dc.cout() << "Hello World!\n";
graphlab::mpi_tools::finalize();
}

