/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

//Modified at 6:35 PM 09/29/2016 by Jay D. Bodra. Search for "Fall 2016" to see the changes
// Fall 2016[jay]. Removed the TxType that was provided. Now is it initialized once in the constructor

extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort(long, char);   //commit/abort based on char value
extern void *process_read_write(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

//Fall 2016[jay]. Modified zgt_tx() in zgt_tx.h
zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2016[jay] R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    //Fall 2016[jay]. writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%d\t%c\tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  struct param *node = (struct param*)arg;// get tid and objno and count

  //do the operations for reading. Write your code
  start_operation(node->tid,node->count);
  //performing p operation
  zgt_p(0);
	zgt_tx *txRead = get_tx(node->tid);
  int tid = txRead->tid;
  //4 states
  //TR_ACTIVE or processing (represented as “P”)
  //TR_WAIT (represented as “W”)
  //TR_ABORT (represented as “A”) and
  // TR_END (represented as “E”)
	if (txRead!=NULL)
	{
		if(txRead->status==TR_ACTIVE)
		{
      //active
      txRead->set_lock(node->tid,1,node->obno,node->count,'S');
			zgt_v(0);
      //fprintf(ZGT_Sh->logfile, "T%d\t\t%cReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t\tP\n", 
      //node->tid, node->Txtype, node->obno, node->count, ZGT_Sh->optime[node->tid]);
      //fflush(ZGT_Sh->logfile);
			finish_operation(txRead->tid);
			pthread_exit(NULL);
		}
    else if(txRead->status==TR_WAIT)
      {
        //waiting
        do_commit_abort(txRead->tid,TR_WAIT);
        zgt_v(0);
        finish_operation(txRead->tid);
        pthread_exit(NULL);
      }
    else if(txRead->status==TR_ABORT)
		{
      //abort
      do_commit_abort(txRead->tid, TR_ABORT);
			zgt_v(0);
			finish_operation(txRead->tid);
			pthread_exit(NULL);
		 } 		 
    else if(txRead->status==TR_END)
    {
        //end
      do_commit_abort(txRead->tid,TR_END);
      zgt_v(0);
      finish_operation(txRead->tid);
      pthread_exit(NULL);
    }
	}
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
  struct param *node = (struct param*)arg;	// struct parameter that contains
  //do the operations for writing; similar to readTx. Write your code
  start_operation(node->tid,node->count);
  //performing p operation
  zgt_p(0);
	zgt_tx *txWrite = get_tx(node->tid);
  //int tid = node->tid;
  //4 states
  //TR_ACTIVE or processing (represented as “P”)
  //TR_WAIT (represented as “W”)
  //TR_ABORT (represented as “A”) and
  // TR_END (represented as “E”)
	if (txWrite != NULL)
	{
		if(txWrite->status==TR_ACTIVE)
		{
      //active
      txWrite->set_lock(node->tid,1,node->obno,node->count,'X');
			zgt_v(0);
      //ZGT_Sh->optime = 0;
      //fprintf(ZGT_Sh->logfile, "T%d\t\t\t%c \t WriteTx \t\t\t%d:%d:%d\t\t  X\t\t\tGranted\t\t\tP\n", 
      //node->tid, node->Txtype, node->obno, node->count, ZGT_Sh->optime[node->tid]);
      //fprintf(ZGT_Sh->logfile, "T%d\t\t%cWriteTx\t%d:%d:%d\t\tWriteLock\tGranted\t\tP\n", 
      //node->tid, node->Txtype, node->obno, node->count, ZGT_Sh->optime[node->tid]);
      //fflush(ZGT_Sh->logfile);
			finish_operation(txWrite->tid);
			pthread_exit(NULL); 
		}
    else if(txWrite->status==TR_WAIT)
      {
        //waiting
        do_commit_abort(txWrite->tid,TR_WAIT);
        zgt_v(0);
        finish_operation(txWrite->tid);
        pthread_exit(NULL);
      }
    else if(txWrite->status==TR_ABORT)
		 {
      //abort
      do_commit_abort(txWrite->tid,TR_ABORT);
			zgt_v(0);
			finish_operation(txWrite->tid);
			pthread_exit(NULL); 
		 }
    else if(txWrite->status==TR_END)
		  {
        //end
				do_commit_abort(txWrite->tid,TR_END);
        zgt_v(0);
        finish_operation(txWrite->tid);
        pthread_exit(NULL);
			}
	}
}

//common method to process read/write: just a suggestion

void *process_read_write(long tid, long obno,  int count, char mode){

  
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;// get tid and count  
  //write your code
    start_operation(node->tid, node->count);
    //p operation
    zgt_p(0);
    //calling fn. with status "Abort"     
    do_commit_abort(node->tid,'A');
    //v opeartion
    zgt_v(0);      
    finish_operation(node->tid);
    pthread_exit(NULL);
}

void *committx(void *arg)
{
 
    //remove the locks/objects before committing
  struct param *node = (struct param*)arg;// get tid and count
  //write your code
  start_operation(node->tid, node->count);
  //performing p opeartion
  zgt_p(0);      
  zgt_tx *txCommit = get_tx(node->tid);
  //changing the status of the transaction to TR_END
  txCommit->status = 'E';
  do_commit_abort(node->tid,'E');
  //performing v operation before finishing
  zgt_v(0);      
  finish_operation(node->tid);
  pthread_exit(NULL);			// thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existant tx

void *do_commit_abort(long t, char status){
  
  // write your code
  int txCount, txWaiting;
  zgt_tx *txCommitAbort = get_tx(t);
  if(txCommitAbort != NULL)
  {
    txCommitAbort->status = status;
    txCommitAbort->free_locks();
    txCount = txCommitAbort->semno;
    //semno +ve -> in use
    //semno -ve -> no one is waiting
    if(txCount != -1)
    {
      txWaiting = zgt_nwait(txCount);
      for(int i = 1; i <= txWaiting; i++)
      {
        zgt_v(txCount);
      }
    }
    txCommitAbort->remove_tx();
  }
	if(status == TR_ABORT)
  {
	  fprintf(ZGT_Sh->logfile, "T%d\t\tAbortTx\n",t);
    fflush(ZGT_Sh->logfile);
  }
	else 
  {
    fprintf(ZGT_Sh->logfile, "T%d\t\tCommitTx\n",t);
	  fflush(ZGT_Sh->logfile);
  }  
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1
  
  //write your code
  //First we check if the obj is present in hash table
  //If present, we find the tx, if the same tx is holding the obj we go ahead if not we make it wait 
  zgt_tx *txSetLock = get_tx(tid1);

  //finding obj in hash table
  zgt_hlink *tmObj = ZGT_Ht->find(sgno1,obno1); 

  if(tmObj != NULL)
  // object present 
  {
      zgt_hlink *tx;
      //finding the tx obj belong to
      tx = ZGT_Ht->findt(this->tid, sgno1, obno1);
	    if(tx != NULL)
      {
        //printf(ZGT_Sh->logfile, "\tTx belonging to obj found");
        //fflush(stdout);
        status = TR_ACTIVE;
        tmObj->lockmode = lockmode1;
        this->perform_readWrite(tid, obno1, lockmode1);
        zgt_v(0);
      }
      else
      {
        zgt_hlink *txWait;
        txWait = this->others_lock(tmObj, sgno1, obno1);
        if(txWait != NULL)
        {
          //making tx wait 
          txSetLock->status = TR_WAIT;
          txSetLock->setTx_semno(txWait->tid, txWait->tid);
          txSetLock->obno = obno1;
          txSetLock->lockmode = lockmode1;
          zgt_v(0);
          zgt_p(txWait->tid);

          //changing to active
          txSetLock->status = TR_ACTIVE;
          zgt_p(0);
          set_lock(txSetLock->tid, txSetLock->sgno, txSetLock->obno, count, txSetLock->lockmode);
          //fprintf(ZGT_Sh->logfile, "%c\tGranted\t%c\t", txSetLock->lockmode, txSetLock->status);
          //fflush(ZGT_Sh->logfile);
        }
        else
        { 
          //printf(ZGT_Sh->logfile, "\tLock not granted");
          //fflush(stdout);
          return 0;
        }
      }
  }
  else
  {
    //object not present, adding it to the hash table
    ZGT_Ht->add(txSetLock, sgno1, obno1, lockmode1);
    perform_readWrite(tid1, obno1, lockmode1);
    zgt_v(0);
    return 0;
  }
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      //fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      //fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   //printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n", temp->tid, temp->obno, temp->lockmode);
	   //fflush(stdout);
#endif
      }
    }
  //fprintf(ZGT_Sh->logfile, "\n");
  //fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2016: not used
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the acutual read/write operation
// based  on the lockmode

void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){
  
  //write your code
	 zgt_tx *node = get_tx(tid);
  int value = ZGT_Sh->objarray[obno]->value;

		 if(lockmode == 'S'){
			ZGT_Sh->objarray[obno]->value = value - 1;
			fprintf(ZGT_Sh->logfile, "T%d\t\tReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t%c\n", 
      this->tid, this->obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
      fflush(ZGT_Sh->logfile);
		}

    else if (lockmode == 'X') {
			ZGT_Sh->objarray[obno]->value = value + 1;
			fprintf(ZGT_Sh->logfile, "T%d\t\tWriteTx\t%d:%d:%d\t\tWriteLock\tGranted\t%c\n", 
      this->tid, this->obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
      fflush(ZGT_Sh->logfile);
      
		}	
}

// routine to check if ant Tx holds the lock on the same object
zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
{
  zgt_hlink *txLock;
  txLock=ZGT_Ht->find(sgno1,obno1);
  while (txLock != NULL)		
  {
      if((txLock->obno == obno1) && (txLock->sgno == sgno1) && (txLock->tid != this->tid)){
	      return(txLock);
      }
      else  {
        txLock = txLock->next;
      }		
    }					
  return(NULL); 
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}



