using namespace std;
void tokenize(string &Sentence, char Delimiter, vector<string> &Words)
{
size_t start;
    size_t end = 0;
 
    while ((start = Sentence.find_first_not_of(delim, end)) != std::string::npos)
    {
        end = Sentence.find(delim, start);
        Words.push_back(Sentence.substr(start, end - start));
    }
}

typedef struct Split_Arguments
{
vector<vector<string>>* PartitionedDataset;
int datasetIndex;
vector<vector<string>>* Tokens;
KeyLockHashMap* KeyObj;

} Split_Arguments;

void DivideData(ifstream &file,vector<vector<string>> &PartitionedData, int NumberOfPartitions, int PartitionSize)
{
string line;
int partitionIndex =0;
int rowIndex =0;
File.open(("Dataset.txt"));
while(std::getline(File, line)) {
//Get the a line from file
//Decide which partition this line will go to
//decide which row this line will go to
//decide when to change the partition
if(PartitionedData[partitionIndex].size() < PartitionSize){
PartitionedData[partitionIndex][rowIndex].push_back(line);
rowIndex++;
}
else{
partitionIndex++;
rowIndex = 0;
PartitionedData[partitionIndex][rowIndex].push_back(line);
rowIndex++;
}
} 


File.close();


}

void* SplitParallel(void* args)
{
Split_Arguments* arg =  (Split_Arguments*)args;
    vector<vector<string>> &PartitionedDataset = *(arg -> PartitionedDataset);
        vector<vector<string>> &Tokens = *(arg -> Tokens);
        int index= *(arg -> datasetIndex);
        vector<string>* lines = &PartitionedDataset[index];
        vector<string>* words = &Tokens[index];
        int numberOfThreads = PartitionedDataset.size();
        KeyLockHashMap* keyObj = *(arg -> KeyObj);

int i, wordCount =0;

for(int j=0;j< lines.size();j++){
vector<string> keys;
tokenize(lines[j],' ', keys); //Foreach Line get all the words

for(int k=0; k< keys.size(); k++){
words[k+wordCount].push_back(keys[k]); //Put all the words from the line above into &words vector
}
wordCount = words[i].size(); //Increment wordCount for next iteration
}

for(int j = 0; j< words.size();j++)
{
keyObj.IncrementKey(words[j]);
} 

}

void Split(vector<vector<string>> &PartitionedDataset,vector<vector<string>> &Tokens,KeyLockHashMap* HashMap,int NumberOfThreads)
{
pthread_t ThreadArray[NumberOfThreads];
KeyLockHashMap* keyObj = new KeyLockHashMap(NumberOfThreads);

Split_Arguments* Split_ArgumentVector[NumberOfThreads];           
    for(int ThreadID=0;ThreadID<NumberOfThreads;ThreadID++) {
    
       Split_Arguments* ThreadArgument = new Split_Arguments;          
        ThreadArgument -> PartitionedDataset  = &PartitionedDataset;
        ThreadArgument -> datasetIndex = ThreadID;     
        ThreadArgument -> Tokens = &Tokens;
  ThreadArgument -> KeyObj = &keyObj;
       Split_ArgumentVector[ThreadID]=ThreadArgument;
        pthread_create(&ThreadArray[ThreadID],NULL,SplitParallel,(void*)Split_ArgumentVector[ThreadID]);
        }
    for(int ThreadID=0;ThreadID<NumberOfThreads;ThreadID++)    
        pthread_join(ThreadArray[ThreadID],NULL);
        
        &HashMap = &Tokens;
        

}
void KeyLockHashMap::IncrementKey(string key)
{
int hashValue =  returnHash(key);
Lock_This_Key(hashValue);
if(HashMap[hashValue].find(key) == HashMap[hashValue].end())
   {
    keyObj.HashMap[hashValue].insert(key,1);
   }
   else
   {
    keyObj.HashMap[hashValue][key] +=1;
   } 
Unlock_This_Key(hashValue);

}
