# Openedge to SQLite dump tool

1. Why did I do this when I can just use `proutil` to the same effect?

   In short, because I can and I'm working on mirroring this production database to another host to run analytics on it.

2. What can this program do?

   Not much other than pull data. The way it's designed (currently) is to parse the metadata from an Openedge table, create its SQLite twin, and then migrate the data using SELECT and INSERT statements. It's very simple. 

3. What is the goal of this project?

   This gets tricky here because our source database had a poor designer, so it lacks traditionally beneficial things like consistently unique indexes and primary keys. In fact, our source database has no reliable change detection mechanism, which is forcing us to use a non-trivial means to track updates. So in short, the goal is reliable 'delta sync' where we are up to date in near realtime for our model's accuracy.


If you have any comments, questions, or suggestions, I'm all ears. Cheers
