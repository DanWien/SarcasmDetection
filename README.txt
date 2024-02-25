Instructions on how to run the project:
Extract the files required to run the system
Open the AWS folder in your desired IDE
Open the App class ---> Edit Run Configurations ---> Add new Run Configuration "App"
In program arguments provide the following:
	a.inputFile names and corresponding outputFile names(Note that the input files must reside in the AWS folder)
	b.The "n" variable indicating how many reviews a worker can process at a time.
	c.Termination flag for the Manager - "y" to terminate , any other character to remain in a running state(must provide some character).
Set the main class to "com.myproject.App"
Inside environment variables provide the AWS credentials to be able to access AWS services.
Once all of the above is completed, the system is ready to be run.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
On a local application, hit run (with the desired input arguments) in the IDE you are using 
and the rest will be done by the AWS platform.
After the system finishes running , the application closes, and the manager is turned off according to the terminate
argument entered as input - "y" will terminate the manager, and any other character will not terminate the manager.
How the system works:
1.Local Application starts running.
2.Local Application creates a designated bucket for itself (each local app creates a personal bucket)
3.Local Application checks for the existence of a manager - if there is a manager ec2 node, the application will proceed, else it will start a new 
manager ec2 node.
4.Local application uploads the input files to handle to its corresponding bucket and waits for a message from the Manager.
5.Manager started running in step 3 - executes the input script i.e makes sure the system where he is running is up to date and starts executing the 
manager's JAR file.
6.Manager creates 4 Queues:
	a.localsQueueUrl : responsible for (a part of the) communication between local apps and the manager - the local apps notify the manager they have uploaded
	their input files and provides the location for the manager to proceed working on the files.
	b.workersTaskQueueUrl : when the manager has split the input files he received into smaller units (specifically n/2) he notifies the workers that the files
	to work on are available to process.
	c.workersAnswersQueueUrl : when a worker finishes working on a file and processing it , he returns the file to the manager.
	d.managerSummaryQueueUrl : when all the parts of an input file were processed , the result is handed back to the local application.
7.Manager creates a synchronized HashMap<String,JobTracker> that is responsible for tracking the work done on a certain file. Each JobTracker is responsible for 
one input file. The JobTracker class purpose is to monitor the work on a certain input file - once the manager processes a file , it is split into smaller tasks
to be handled by workers, and we need to keep track of how many parts have been handled to know when a certain file was fully processed.
8.Manager creates a ThreadPool , and assigns a thread to each of his duties : 
	a.Listening to local applications (provide the ability to take in more than one request at a time)
	b.Process messages coming from local applications - where the input files are located and providing a more convenient structure for splitting.
	c.Split the input files and start worker nodes to handle the jobs.
	d.Receive messages from workers that indicate completion of their proccessed file parts.
Manager workflow : Initiate all the parts we described above ---> Take in a message from local application ---> 
---> Split the input files to appropriate smaller sized files ---> Start worker nodes ---> Listen to the workersAnswersQueueUrl queue --->
---> Once a given JobTracker indicates an input file was fully processed, aggregate the results and forward them to the local app ---> repeat until work is done ---> 
---> If the terminate flag was raised, we have a shutdownProcess() that waits for all the worker nodes to terminate, then ensures our JobTracker map is empty , 
and then begins deleting all the Queues, shuts down its executor service and finishes gracefully.
Worker workflow : The worker has 2 missions -
	a.Work on the input file he withdrew from the Queue
	b.Ensure that the message related to this file is not polled by configuring the Visibility of the message on the appropriate SQS.
Generally, the worker has a designated method handleJob() where he performs all the required logic to handle the processing. 
In the worker's main method we initiate a SingleThreadPool because we want a second thread that checks if the job is done at a given time : 
if the work is done: do nothing. else: make sure the message remains hidden until it has been processed. 
The worker remains existant for 90 seconds within finishing a job incase there are more incoming jobs, we don't want the workers to terminate immediately 
to avoid rebooting ec2 nodes a lot of times.
9.Once a local application receives all of the input files back after processing them, the local app creates an appropriate HTML file as specified and exits.
Note: 
	a.Every file that was locally created is deleted once it is not needed anymore by the workers.
	b.The Manager delets all Queues upon finishing. 
	c.The local app deletes the bucket that was created.
The cleanup process is complete - no resource is left unattended.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
The ami we used was "ami-00e95a9222311e8ed" as provided.
The type we used was M4.large
The time it takes to process all 5 of the input files at once was around 10-20 minutes.
The n we used was 200 throughout all of our testing.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
Mandatory Requirements:
Our program is scalable, it will properly work under the limitations provided (i.e starts a maximum number of 8 worker nodes) and was tested with 4 different local apps.
Persistence wise, if a worker node stalls for a while because of a heavy job or any other cause, the same worker node ensures the message will not be processed twice
by taking advantage of the visibility mechanism and keeping the message hidden. In case a node dies, the message will go back to being visibile and another worker node
will handle it(The worker dies and so does the thread that is responsible for ensureVisibility())
Threading was explained above.
Termination process was handled.
Our manager is not doing more work than he needs to, by tracking and monitoring his actions using printing, we ensured each thing the manager handles is either easy
to handle (i.e takes no time and isn't heavy) or assigned to a designated thread by the executor service.

Dan Wiener,    209413640
Noa Youlzarie, 209320605
