//****************************************************************************80

//Name : S Sachin Kumar
//SRN : PES1PG21CS032
//MWP ASSIGNMENT 1
//
//Discussion: To Parallelise the Word count.
//            The idea is to divide the data, get the line from the file,and decide which 
//            partiotion it will go to, in the partition which row the line will go to,and 
//            also decide when to change the partition.
//            And in the Partition(all the lines),get all the words from the lines. 

//****************************************************************************80 

using namespace std;

// Create a Strtucture to hold all the arguments for the structure

typedef struct Split_Arguments
{
    // Initialize the Problem data
	vector<vector<string>>* PartitionedDataset;  
	int d_Index;
	vector<vector<string>>* Tkns;

} Split_Arguments;


//**************************************************************************************************************************80
// The Strings are converted to tokens that are then converted to lowercase and put in the words vector.
//**************************************************************************************************************************80

void tokenize(string &Sentence, char Delimiter, vector<string> &Words)
{
	size_t start;
    size_t fin = 0;
 
    while ((start = Sentence.find_first_not_of(delim, fin)) != std::string::npos)
    {
        fin = Sentence.find(delim, start);
        Words.push_back(Sentence.substr(start, fin - start));
    }
}

//**************************************************************************************************************************80
// The Dataset is read, and it is divided into NumberOfPartition's, chunks, each with a size of given by PartitionSize.
//**************************************************************************************************************************80

void DivideData(ifstream &file,vector<vector<string>> &PartitionedData, int NumberOfPartitions, int PartitionSize)
{
    // Initialize Partion index and Row index to Zero.

	string line;
	int pIndex =0;   
	int rIndex =0;
	File.open(("Dataset.txt"));
		while(std::getline(File, line)) {
			
			if(PartitionedData[pIndex].size() < PartitionSize){
				PartitionedData[pndex][rIndex].push_back(line);
				rIndex++;
			}
			else{
				pIndex++;
				rIndex = 0;
				PartitionedData[pIndex][rIndex].push_back(line);
				rIndex++;
			}
		}				
	File.close();
}

//**************************************************************************************************************************80
// In order for each of the thread to call the tokenize(), NumberOfThreads are created and 
// each thread is assigned a vector of the Partitioned data and a vector of tokens
// *************************************************************************************************************************80

void Split(vector<vector<string>> &PartitionedDataset,vector<vector<string>> &Tkns,KeyLockHashMap* HashMap,int NumberOfThreads)
{
	pthread_t ThreadArray[NumberOfThreads];
	Split_Arguments* Split_ArgumentVector[NumberOfThreads];           
    int thread_id;
    for(thread_id=0;thread_id<NumberOfThreads;thread_id++) {
    
       Split_Arguments* ThreadArgument = new Split_Arguments;          
        ThreadArgument -> PartitionedDataset  = &PartitionedDataset;
        ThreadArgument -> d_Index = thread_id;     
        ThreadArgument -> Tkns = &Tkns;
       Split_ArgumentVector[thread_id]=ThreadArgument;
        pthread_create(&ThreadArray[thread_id],NULL,SplitParallel,(void*)Split_ArgumentVector[thread_id]);
        }
    for(thread_id=0;thread_id<NumberOfThreads;thread_id++)    
        pthread_join(ThreadArray[thread_id],NULL);

}


//**************************************************************************************************************************80
// Fucntion that is called by each of the threads 
//**************************************************************************************************************************80

void* SplitParallel(void* args)
{
	Split_Arguments* arg =  (Split_Arguments*)args;
    vector<vector<string>> &PartitionedDataset = *(arg -> PartitionedDataset);
        vector<vector<string>> &Tkns = *(arg -> Tkns);
        int index= *(arg -> d_Index);
        vector<string>* lines = &PartitionedDataset[index];
        vector<string>* words = &Tkns[index];

       
	int i, wCount =0;
	int j, k;
		for(j=0;j< lines.size();j++){
			vector<string> keys;
		tokenize(lines[j],' ', keys); 
		
		for(k=0; k< keys.size(); k++){
			words[k+wCount].push_back(keys[k]); 
		}
			wCount = words[i].size(); 
}



