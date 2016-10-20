#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>

#define ROOT 0
int main(int argc,char** argv){

  
  
  int rank , totalTasks,rc,activeTask;
  int* fullBuffer;
  int* localBuffer;
  int numElement,locBufSize,blockSize;
  int i,j,k;
  int recvSlot1,recvSlot2;
  double time1,time2,ioTime,ioTimeTotal,commuTime,commuTimeTotal; 
  commuTimeTotal = 0;
  ioTimeTotal = 0;
  MPI_File fh , out;
  MPI_Status status;
  MPI_Request req1,req2;

  rc = MPI_Init(&argc,&argv);
  
 time1 = MPI_Wtime();
 printf("OAO\n");
  if(rc!= MPI_SUCCESS){
	printf("Error when initializing mpi \n");
  }

  MPI_Comm_size(MPI_COMM_WORLD,&totalTasks);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

//  time1 = MPI_Wtime();

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
    //            printf("rank %d   %d\n",ranks[i],rank);
	}
	MPI_Group_incl(old_group,totalTasks-activeTask,ranks,&group);
  }

  commuTime = MPI_Wtime();
  MPI_Comm_create(MPI_COMM_WORLD,group,&myComm);
  MPI_Barrier(myComm);
  commuTimeTotal += MPI_Wtime() - commuTime;
  //printf("xxx\n");

if(locBufSize!=0){
	

  fullBuffer = (int*) malloc(numElement * sizeof(int));
  localBuffer = (int*) malloc((locBufSize*sizeof(int)));
  
  
  ioTime = MPI_Wtime();
  MPI_File_open(myComm,argv[2],MPI_MODE_RDONLY,MPI_INFO_NULL,&fh); 
  ioTimeTotal += MPI_Wtime() - ioTime;
 // MPI_File_read(fh,fullBuffer,numElement,MPI_INT,&status);

  int sizeCounter = 0;
  for(i=0;i<rank;i++){
	
	if(i<numElement%totalTasks)
		sizeCounter += atoi(argv[1])/totalTasks+1;
	else	
		sizeCounter += atoi(argv[1])/totalTasks;	
  }

  

 if(rank==0){
  	ioTime = MPI_Wtime();
  	MPI_File_read(fh,fullBuffer,numElement,MPI_INT,&status);
  	ioTimeTotal += MPI_Wtime() - ioTime;
  }

    MPI_File_close(&fh);
  	MPI_Barrier(myComm);

	
	int* sendCount = (int*) malloc(sizeof(int)*activeTask);
	int* disArray = (int*) malloc(sizeof(int)*activeTask);
	disArray[0] = 0;
	for(i=0;i<activeTask;i++){

	if(i>=numElement%totalTasks){
  		sendCount[i] = atoi(argv[1])/totalTasks;
 	 }
 	 else{
  		sendCount[i] = atoi(argv[1])/totalTasks+1;  
 	 }
 	 if(i!=0){
 	 	disArray[i] = disArray[i-1] + sendCount[i];
 	 }
 	}

 	commuTime = MPI_Wtime();
 	MPI_Scatterv(fullBuffer,sendCount,disArray,MPI_INT,localBuffer,locBufSize,MPI_INT,0,myComm);
 	commuTimeTotal += MPI_Wtime() - commuTime1;
  //memcpy(localBuffer,fullBuffer+sizeCounter*sizeof(int),locBufSize*sizeof(int));	
  
  
  
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
  while(1){
	int offset,temp;
	swaped = 0;
	debugctr++;
	
        // ODD - PHASE
        /*
	if(startIndex%2==0){
	  MPI_Isend(&localBuffer[0],1,MPI_INT,rank-1,rank-1,MPI_COMM_WORLD,&req1);
	  MPI_Irecv(&recvSlot1,1,MPI_INT,rank1-1,rank,MPI_COMM_WORLD,&req2);
	}
	if(endIndex%2==1){
	  MPI_Recv(&recvSlot2,1,MPI_INT,rank+1,rank,MPI_COMM_WORLD);
	  if(recvSlot2<localBuffer[locBufSize-1]){
		temp = localBuffer[locBufSize-1];
		localBuffer[locBufSize-1] = recvSlot2;
		recvSlot2 = temp;
	  }
	  MPI_Send(&recvSlot2,1,MPI_INT,rank+1,rank+1,MPI_COMM_WORLD);
	}
	*/
	if(rank%2==0){
	  // Send 1st element to rank x-1
	  if(rank!=0&&startIndex%2==0){
		
		commuTime = MPI_Wtime();
	  	MPI_Send(&localBuffer[0],1,MPI_INT,rank-1,rank-1,myComm);
	//	printf("%d 1\n",rank);
		MPI_Recv(&recvSlot1,1,MPI_INT,rank-1,rank,myComm,&status);
	  	commuTimeTotal += MPI_Wtime() - commuTime;
	//	printf("%d 2\n",rank);
	  	localBuffer[0] = recvSlot1;
	  }
	   // Recv 1st element form rank x+1	  
	 if(rank!=activeTask-1&&endIndex%2==1){

	   commuTime = MPI_Wtime();
           MPI_Recv(&recvSlot2,1,MPI_INT,rank+1,rank,myComm,&status);
           commuTimeTotal += MPI_Wtime() - commuTime;  
	//printf("%d 3 recvSlot2=%d\n",rank,recvSlot2);
           if(recvSlot2<localBuffer[locBufSize-1]){
                temp = localBuffer[locBufSize-1];
                localBuffer[locBufSize-1] = recvSlot2;
                recvSlot2 = temp;
         	swaped = 1;
	    }

	   commuTime = MPI_Wtime();
           MPI_Send(&recvSlot2,1,MPI_INT,rank+1,rank+1,myComm);
           commuTimeTotal += MPI_Wtime() - commuTime;
	  // printf("%d 4\n",rank);
	  }
	}
	else{
	   // Recv 1st element from rank x+1
	    if(rank!=activeTask-1&&endIndex%2==1){
		commuTime = MPI_Wtime();
                MPI_Recv(&recvSlot2,1,MPI_INT,rank+1,rank,myComm,&status);
		commuTimeTotal += MPI_Wtime() - commuTime;
	//	printf("%d a\n",rank);
           if(recvSlot2<localBuffer[locBufSize-1]){
                temp = localBuffer[locBufSize-1];
                localBuffer[locBufSize-1] = recvSlot2;
                recvSlot2 = temp;
		swaped = 1;
           }
		commuTime = MPI_Wtime();
                MPI_Send(&recvSlot2,1,MPI_INT,rank+1,rank+1,myComm);
		commuTimeTotal += MPI_Wtime() - commuTime;    
     //	printf("%d b\n",rank);
	   }
	   // Send 1st element to rank x-1	
	   if(rank!=0&&startIndex%2==0){
		commuTime = MPI_Wtime();
                MPI_Send(&localBuffer[0],1,MPI_INT,rank-1,rank-1,myComm);
          //      printf("%d c\n",rank);
		MPI_Recv(&recvSlot1,1,MPI_INT,rank-1,rank,myComm,&status);
	//	printf("%d d\n",rank);
		commuTimeTotal += MPI_Wtime() - commuTime;
                localBuffer[0] = recvSlot1;
          }
	}
	
		

	if(startIndex%2 == 0)
		offset = 1;
	else 
		offset = 0;


	for(i=offset;i<locBufSize-1;i+=2){
          if(localBuffer[i]>localBuffer[i+1]){
		temp = localBuffer[i];
		localBuffer[i] = localBuffer[i+1];
		localBuffer[i+1] = temp;
		swaped = 1;
	  }
	}


	

  	// EVEN - PHASE


	if(rank%2==0){
	  // Send 1st element to rank x-1
	  if(rank!=0&&startIndex%2==1){
		commuTime = MPI_Wtime();
	  	MPI_Send(&localBuffer[0],1,MPI_INT,rank-1,rank-1,myComm);
		MPI_Recv(&recvSlot1,1,MPI_INT,rank-1,rank,myComm,&status);
	  	commuTimeTotal += MPI_Wtime() - commuTime;
		localBuffer[0] = recvSlot1;
	  }
	   // Recv 1st element form rank x+1	  
	 if(rank!=activeTask-1&&endIndex%2==0){
		commuTime = MPI_Wtime();
                MPI_Recv(&recvSlot2,1,MPI_INT,rank+1,rank,myComm,&status);
          	commuTimeTotal += MPI_Wtime() - commuTime;
           if(recvSlot2<localBuffer[locBufSize-1]){
                temp = localBuffer[locBufSize-1];
                localBuffer[locBufSize-1] = recvSlot2;
                recvSlot2 = temp;
		swaped = 1;
           }
		commuTime = MPI_Wtime();
                MPI_Send(&recvSlot2,1,MPI_INT,rank+1,rank+1,myComm);
         	commuTimeTotal += MPI_Wtime() - commuTime; 	
	 }
	}
	else{
	   // Recv 1st element from rank x+1
	    if(rank!=activeTask-1&&endIndex%2==0){
		commuTime = MPI_Wtime();
                MPI_Recv(&recvSlot2,1,MPI_INT,rank+1,rank,myComm,&status);
		commuTimeTotal += MPI_Wtime() - commuTime;

           if(recvSlot2<localBuffer[locBufSize-1]){
                temp = localBuffer[locBufSize-1];
                localBuffer[locBufSize-1] = recvSlot2;
                recvSlot2 = temp;
		swaped = 1;
           }
		commuTime = MPI_Wtime();
                MPI_Send(&recvSlot2,1,MPI_INT,rank+1,rank+1,myComm);
          	commuTimeTotal += MPI_Wtime() - commuTime;
  	   }
	   // Send 1st element to rank x-1	
	   if(rank!=0&&startIndex%2==1){
		commuTime = MPI_Wtime();
                MPI_Send(&localBuffer[0],1,MPI_INT,rank-1,rank-1,myComm);
                MPI_Recv(&recvSlot1,1,MPI_INT,rank-1,rank,myComm,&status);
		commuTimeTotal += MPI_Wtime() - commuTime;
                localBuffer[0] = recvSlot1;
          }
	}
	
	if(startIndex%2 == 1)
		offset = 1;
	else 
		offset = 0;

	for(i=offset;i<locBufSize-1;i+=2){
          if(localBuffer[i]>localBuffer[i+1]){
		temp = localBuffer[i];
		localBuffer[i] = localBuffer[i+1];
		localBuffer[i+1] = temp;
		swaped = 1;
	  }
        }

	//printf("QQ %d\n",rank);	
	
	commuTime = MPI_Wtime();

	MPI_Allgather(&swaped,1,MPI_INT,swaps,1,MPI_INT,myComm);
	for(i=0;i<activeTask;i++)
		if(swaps[i]==1)
			swaped = 1;
	
	//printf("OAO %d\n",rank);
	MPI_Barrier(myComm);
//	commuTimeTotal += MPI_Wtime() - commuTime;
	if(swaped == 0)
		break;
	
        commuTimeTotal += MPI_Wtime() - commuTime;

  }
     	//printf("QQQ\n");
	
	ioTime = MPI_Wtime();
	MPI_File_open(myComm,argv[3],MPI_MODE_WRONLY|MPI_MODE_CREATE,MPI_INFO_NULL,&out);
	ioTimeTotal += MPI_Wtime() - ioTime;

	commuTime = MPI_Wtime();
	MPI_Gatherv(localBuffer,locBufSize,MPI_INT,fullBuffer,sendCount,disArray,MPI_INT,0,myComm);
	commuTimeTotal += MPI_Wtime() - commuTime;

	if(rank==0){
		ioTime = MPI_Wtime();
		MPI_File_write(out,fullBuffer,numElement,MPI_INT,&status);
		ioTimeTotal += MPI_Wtime() - ioTime;
	}

	commuTime = MPI_Wtime();
	MPI_Barrier(myComm);
	commuTimeTotal += MPI_Wtime() - commuTime;
	//MPI_File_write_at(out,sizeCounter*sizeof(int),localBuffer,locBufSize,MPI_INT,&status);
	MPI_File_close(&fh);
	ioTimeTotal += MPI_Wtime() - ioTime;
}

//  MPI_Finalize();

  time2 = MPI_Wtime();
  printf("rank %d  time = %lfcompute time =%lf commu time = %lf io time=%lf\n",rank,(time2-time1),(time2-time1-ioTimeTotal-commuTimeTotal),commuTimeTotal,ioTimeTotal);
  MPI_Finalize();
  return 0;

}
