# Data-Analysis-Project-using-Python-and-MySQL
Questions File contain : 
Columns:
	_id: Unique id of question,
	userId: Userid who has asked the question,
	creationDate: Time when question was created,
	Format of time: Epoch/unix time,
	isAnonymous: Tells if the question has been asked anonymously,
	True: Asked anonymously,
	False: Not Asked anonymously,
	isDeleted: Tells if the question has been deleted,
	True: Question is deleted by the user,
	False: Question is not deleted by the user
	
	
	
Answers File contain: 
Columns:
	parentId: Unique id of question that was answered,
	userId: User who has answered the question,
	creationDate,
	Time when answered was posted,
	Format of time: Epoch/unix time

Questions for Data Analysis:-
1.	Total number of answered questions which have been deleted
2.	Total number of answered questions which have posted anonymously
3.	Date when most number of questions were asked anonymously
4.	Percentage of questions answered after 5 minutes of creation
5.	Median Turn Around Time of answers (Turn Around Time: Total time taken to answer the question)
6.	First question of all users which are unanswered





