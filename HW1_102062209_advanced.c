#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>

void quickSort(int* array,int left,int right);

int compare(const void *a, const void *b)
{
      int c = *(int *)a;
      int d = *(int *)b;
      if(c < d) {return -1;}               
      else if (c == d) {return 0;}      
      else return 1;                          
}


int main(int argc,char** argv){


  
  int rank , totalTasks,rc,activeTask;
  int* fullBuffer;
  int* localBuffer;
  int* finalBuffer;
  int numElement,locBufSize,blockSize;
  int i,j,k;
  int recvSlot1,recvSlot2;
  double time1,time2;
  double commuTime1 ,commuTime2,commuTimeTotal;
  double ioTime1,ioTime2,ioTimeTotal;
  int serialIO;
  commuTimeTotal = 0;
  ioTimeTotal = 0;
  serialIO = 0;
 
  //return 0 ;
  printf("aaa\n");
  MPI_File fh , out;
  MPI_Status status;
  MPI_Request req1,req2;

  rc = MPI_Init(&argc,&argv);

  time1 =MPI_Wtime();
  if(rc!= MPI_SUCCESS){
	printf("Error when initializing mpi \n");
  }

  MPI_Comm_size(MPI_COMM_WORLD,&totalTasks);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);



  numElement = atoi(argv[1]);
  if(rank>=numElement%totalTasks){
  	locBufSize = atoi(argv[1])/totalTasks;
  }
  else{
  	locBufSize = atoi(argv[1])/totalTasks+1;  
  }
  if(totalTasks>numElement){
	activeTask = numElement;
  }
  else{
	activeTask = totalTasks;
  }
  //blockSize = atoi(argv[0]/totalTasks)+1;
  printf("aaa activeTask=%d  locBufSize = %d   totalTasks = %d  argv1=%d  \n",activeTask,locBufSize,totalTasks,atoi(argv[1]));

  
  MPI_Group old_group,group;
  MPI_Comm myComm;
  MPI_Comm_group(MPI_COMM_WORLD,&old_group);

  int* ranks; 

  if(rank<activeTask){
	ranks = (int*) malloc(sizeof(int)*activeTask);
  	for(i=0;i<activeTask;i++){
		ranks[i]=i;
//		printf("rank %d   %d\n",ranks[i],rank);
  	}
	MPI_Group_incl(old_group,activeTask,ranks,&group);
  }
  else{
	ranks = (int*) malloc(sizeof(int)*(totalTasks-activeTask));
	for(i=0;i<(totalTasks-activeTask);i++){
		ranks[i]=i+activeTask;
  //              printf("rank %d   %d\n",ranks[i],rank);
	}
	MPI_Group_incl(old_group,totalTasks-activeTask,ranks,&group);
  }


  MPI_Comm_create(MPI_COMM_WORLD,group,&myComm);


  commuTime1 = MPI_Wtime();
  MPI_Barrier(myComm);
  commuTimeTotal += MPI_Wtime() - commuTime1;
  //printf("xxx\n");

if(locBufSize!=0){
	

  fullBuffer = (int*) malloc(numElement* sizeof(int));
  localBuffer = (int*) malloc((locBufSize*sizeof(int)));
  
  

  ioTime1 = MPI_Wtime();
  MPI_File_open(myComm,argv[2],MPI_MODE_RDONLY,MPI_INFO_NULL,&fh); 
 // MPI_File_read(fh,fullBuffer,numElement,MPI_INT,&status);
  ioTimeTotal += MPI_Wtime() - ioTime1;

  int sizeCounter = 0;
  


  for(i=0;i<rank;i++){
	
	if(i<numElement%totalTasks)
		sizeCounter += atoi(argv[1])/totalTasks+1;
	else	
		sizeCounter += atoi(argv[1])/totalTasks;	
  }

  MPI_File_read_at(fh,sizeCounter*sizeof(int),localBuffer,locBufSize,MPI_INT,&status);
   
  /*
  if(rank==0){
  	memcpy(localBuffer,finalBuffer,sizeof(int)*locBufSize);
  	MPI_Send(finalBuffer,numElement,MPI_INT,rank+1,rank+1,myComm);
  }else{
  	MPI_Recv(finalBuffer,numElement,MPI_INT,rank-1,rank,myComm,&status);
  	memcpy(localBuffer,&finalBuffer[sizeCounter],locBufSize);
  	MPI_Send(finalBuffer,numElement,MPI_INT,rank+1,rank+1,myComm);
  }
  commuTimeTotal += MPI_Wtime() - commuTime;
   MPI_Barrier(myComm);
  printf("rank=%d  sizeCounter=%d \n",rank,sizeCounter);
  for(i=0;i<locBufSize;i++){
    printf("rank%d [%d]=%d \n",rank,i,localBuffer[i]);
  }
*/

  //bool oddPhase = true;
  int swaped = 0;
  int* swaps = (int*) malloc(sizeof(int)*activeTask);
  //bool evenBlock;
  //int a = blockSize % 2;
  int startIndex,endIndex;
  startIndex = sizeCounter + 1;
  endIndex = sizeCounter + locBufSize;

  int debugctr = 0;
  //printf("bbb localBuffer[0]=%d   sizeCounter = %d   startIndex=%d   endIndex=%d \n",localBuffer[0],sizeCounter,startIndex,endIndex);
  
	

	qsort((void*)localBuffer,locBufSize,sizeof(int),compare);
	//quickSort(localBuffer,0,locBufSize-1); // sort the local buffer in each process
//	printf("rankdd %d \n",rank);
//	for(i=0;i<locBufSize;i++)
//		printf("rank:%d [%d]=%d \n",rank,i,localBuffer[i]);
	int half,sendNum,recvNum;
	half = (activeTask%2)==0 ? activeTask/2:(activeTask/2 + 1);
	
	/*
	if( numElement % activTask != 0 && activeTask==totalTasks){

	if(rank == (numElement % activeTask)){
		if(rank<activeTask/2){
			sendNum = locBufSize - 1;
			recvNum = locBufSuze;
		}
		else{
			sendNum = locBufSize + 1;
			recvNum = locBufSize;
		}
	}
	else if(rank ==(numElement % activeTask)-1){
		 if(rank<activeTask/2){
                        sendNum = locBufSize - 1;
                        recvNum = locBufSuze;
                }
                else{
                        sendNum = locBufSize + 1;
                        recvNum = locBufSize;
                }
	}
	else{
		sendNum = locBufSize;
		recvNum = locBufSize;
	}

	}
	else{

		sendNum = locBufSize;
		recvNum = locBufSize;
	}
	*/
	recvNum = 0;
	int* recvBuffer ;//= (int*) malloc(sizeof(int)*recvNum);
	int* mergeBuffer;// = (int*) malloc(sizeof(int)*(recvNum+locBufSize);
	int a,b;
	if(rank!=0 && rank!=activeTask-1){
		if(rank<half){
			
			commuTime1 = MPI_Wtime();
			MPI_Recv(&recvNum,1,MPI_INT,rank-1,rank,myComm,&status);
			commuTimeTotal += MPI_Wtime() - commuTime1;

	   		recvBuffer = (int*) malloc(sizeof(int)*recvNum);
		       // mergeBuffer = (int*) malloc(sizeof(int)*(recvNum+locBufSize));
		        commuTime1 = MPI_Wtime();
			MPI_Recv(recvBuffer,recvNum,MPI_INT,rank-1,rank,myComm,&status);
			 commuTimeTotal += MPI_Wtime() - commuTime1;

		}
		else{
			commuTime1 = MPI_Wtime();
			MPI_Recv(&recvNum,1,MPI_INT,rank+1,rank,myComm,&status);
			commuTimeTotal += MPI_Wtime() - commuTime1;

			recvBuffer = (int*) malloc(sizeof(int)*recvNum);
                        //mergeBuffer = (int*) malloc(sizeof(int)*(recvNum+locBufSize));
			commuTime1 = MPI_Wtime();
			MPI_Recv(recvBuffer,recvNum,MPI_INT,rank+1,rank,myComm,&status);
			commuTimeTotal += MPI_Wtime() - commuTime1;
		}
	}

	//for(i=0;i<recvNum;i++)
	//	printf("rank%d recv[%d]=%d\n",rank,i,recvBuffer[i]);	
	//printf("rank:%d aaaaa locbufsize=%d  recvNum=%d\n",rank,locBufSize,recvNum);
	mergeBuffer = (int*) malloc(sizeof(int)*(locBufSize+recvNum));
	// MERGE
	a = 0;
	b = 0;
	for( i=0;i<(recvNum+locBufSize);i++){
		if(a>=recvNum){
			mergeBuffer[i] = localBuffer[b++];	
		}
		else if(b>=locBufSize){
			mergeBuffer[i] = recvBuffer[a++];
		}else{

		if(localBuffer[b]>recvBuffer[a]){
			mergeBuffer[i] = recvBuffer[a++];
		}
		else{
			mergeBuffer[i] = localBuffer[b++];
		}

		}
	}
	
	//for(i=0;i<recvNum+locBufSize;i++)
	//	printf("merge rank %d [%d]=%d \n",rank,i,mergeBuffer[i]);
	
	//printf("rank:%d bbb\n",rank);
		
        //MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);


	if(rank!= (half-1)){
		if(rank<half&&rank!=activeTask-1){
			sendNum = recvNum + locBufSize;
			
			commuTime1 = MPI_Wtime();
			MPI_Send(&sendNum,1,MPI_INT,rank+1,rank+1,myComm);
			MPI_Send(mergeBuffer,sendNum,MPI_INT,rank+1,rank+1,myComm);
			commuTimeTotal += MPI_Wtime() - commuTime1;
		}
		else if(rank!=0){
			sendNum = recvNum + locBufSize;
			commuTime1 = MPI_Wtime();
			MPI_Send(&sendNum,1,MPI_INT,rank-1,rank-1,myComm);
			MPI_Send(mergeBuffer,sendNum,MPI_INT,rank-1,rank-1,myComm);
			commuTimeTotal += MPI_Wtime() - commuTime1;
		}

	//	 MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
	
	}
	else{	
		

		int finalNum = 0;
		int* tempBuffer;
		if(rank!=activeTask-1){
		  commuTime1 = MPI_Wtime();
		  MPI_Recv(&finalNum,1,MPI_INT,rank+1,rank,myComm,&status);
		  commuTimeTotal += MPI_Wtime() - commuTime1;
		}
		tempBuffer = (int*) malloc(sizeof(int)*finalNum);
		finalBuffer = (int*) malloc(sizeof(int)*numElement);
		
		if(rank!=activeTask-1){
		commuTime1 = MPI_Wtime();
		MPI_Recv(tempBuffer,finalNum,MPI_INT,rank+1,rank,myComm,&status);
		commuTimeTotal += MPI_Wtime() -commuTime1;
		}
		int x = 0, y = 0;
		//printf("111 finalNum:%d \n",finalNum);
		//for(i=0;i<finalNum;i++)
		//	printf("[%d]=%d \n",i,tempBuffer[i]);
		for(i=0;i<numElement;i++){
			if(x>=finalNum){
				finalBuffer[i] = mergeBuffer[y++];
			}
			else if(y>=(recvNum+locBufSize)){
				finalBuffer[i] = tempBuffer[x++];
			}
			else{
				if(mergeBuffer[y]>tempBuffer[x]){
				finalBuffer[i] = tempBuffer[x++];
				}
				else{
				finalBuffer[i] = mergeBuffer[y++];
				}
			}
		//	printf("finalBuffer [%d] = %d x=%d  y=%d\n",i,finalBuffer[i],x,y);
		}
		//printf("222\n");
	//	MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
        //	MPI_File_write(out,finalBuffer,numElement,MPI_INT,&status);
        //	MPI_File_close(&fh);


	}
	  
	 int* outBuffer;
   
	 
	 if(serialIO==1){
	 	ioTime1 = MPI_Wtime();
	 MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
         if(rank == (half-1)){
	 	MPI_File_write(out,finalBuffer,numElement,MPI_INT,&status);
	 }
         MPI_File_close(&fh);
         ioTimeTotal += MPI_Wtime() - ioTime1;
	 }
	 else{


	 	commuTime1 = MPI_Wtime();
	 	if(rank==half-1){

	 		if(rank!=0){
	 		MPI_Send(finalBuffer,sizeCounter,MPI_INT,rank-1,rank-1,myComm);
	 		}
	 		if(rank!=activeTask-1){
	 		MPI_Send(&finalBuffer[sizeCounter+locBufSize],numElement-sizeCounter-locBufSize,MPI_INT,rank+1,rank+1,myComm);
	 		}
	 		memcpy(localBuffer,&finalBuffer[sizeCounter],locBufSize);
	 	}
	 	else if(rank<half-1){
	 		MPI_Recv(fullBuffer,sizeCounter+locBufSize,MPI_INT,rank+1,rank,myComm,&status);
	 		if(rank!=0){
	 		MPI_Send(fullBuffer,sizeCounter,MPI_INT,rank-1,rank-1,myComm,&status);
	 		}
	 		memcpy(localBuffer,&fullBuffer[sizeCounter],locBufSize);
	 	}
	 	else if(rank>half-1){
	 		MPI_Recv(fullBuffer,numElement-sizeCounter,MPI_INT,rank-1,rank,myComm,&status);
	 		if(rank!=activeTask-1){
	 		MPI_Send(&fullBuffer[locBufSize],numElement-sizeCounter-locBufSize,MPI_INT,rank+1,rank+1,myComm,&status);
	 		}
	 		memcpy(localBuffer,&fullBuffer[0],locBufSize);
	 	}

	 	commuTimeTotal += MPI_Wtime() - commuTime1;

		ioTime1 = MPI_Wtime();
		 MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
		 MPI_File_write_at(out,sizeCounter*sizeof(int),localBuffer,locBufSize,MPI_INT,&status);
		

		 MPI_File_close(&fh);
		 ioTimeTotal += MPI_Wtime() - ioTime1;


	 }
	 
	//printf("rank:%d ccc\n",rank);  
     	//printf("QQQ\n");
	//for(i=0;i<locBufSize;i++)
		//printf("rank%d  [%d]=%d\n",rank,i,localBuffer[i]);

	//MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
	//MPI_File_write_at(out,sizeCounter*sizeof(int),localBuffer,locBufSize,MPI_INT,&status);
	//MPI_File_close(&fh);
}

  time2 = MPI_Wtime();
  printf("rank%d time=%lf  commpute time:%lf   commu time:%lf  io time:%lf \n",rank,time2-time1,(time2-time1-commuTimeTotal-ioTimeTotal),commuTimeTotal,ioTimeTotal);
  MPI_Finalize();
  
  return 0;

}



void quickSort(int* array,int left,int right){
 
  if(left<right){

	
	int i,j;
	i = left + 1;
	j = right;
	while(1){

	while(i<=right &&  array[i] <  array[left])i++;
	while(j>=left && array[j] > array[left]) j--;

	if(j<=i)break;
	
	int temp = array[i];
	array[i] = array[j];
	array[j] = temp;

	}
	int temp = array[left];
	array[left] = array[j];
	array[j] = temp;
	quickSort(array,left,j-1);
	quickSort(array,j+1,right);

  }

  return;
}
