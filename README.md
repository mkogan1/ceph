# Accelerating Ceph Cloud Storage with D4N
#### **Team members:** Daniel Paganelli, Khai Evdaev, Mandy Yao, Samarah Uriarte
#### **Mentors:** Ali Maredia, Amin Mosayyebzadeh, Mark Kogan, Matt Benjamin, Emine Ugur Kaynar

---
### How to Run the Code
#### Set Up
1. Add the following to your .ssh config file:
<!-- -->

	Host gateway
		Hostname 128.31.26.122
		User centos
		ForwardAgent yes
	
		#Port 22

	Host Ceph2
		Hostname 192.168.0.106
		User centos
		ForwardAgent yes
		ProxyCommand ssh gateway -A -Y -W %h:%p

2. Ensure your public SSH key has been added to the Ceph2 VM.
3. SSH into Ceph2 through a terminal.

#### Program Execution and Testing
4. Navigate to the /home/centos/ceph/build/ directory.
5. Run the full_script.sh script. This script will kill any currently running Ceph clusters, build a new one, follow the D4N caching and directory process, and save all the directory keys, as well as the RGW bucket keys, into two separate files for testing.<br />
<!-- -->

	./full_script.sh
6. Compile the unit test C++ program.
<!-- -->

	g++ -o unitTest rgw_directory_unitTest.cc
8. Run the unit test executable.<br />
<!-- -->

	./unitTest
8. Ensure the output confirms the keys match.
9. Kill the Ceph cluster by running the stop.sh script.<br />
<!-- -->
    
	../src/stop.sh
10. Exit your SSH session.

---
### Project Overview
With data volumes growing exponentially, a highly scalable storage that preserves and records digital content for ongoing or future company operations is a paramount solution to any successful business. Red Hat Ceph, an open-source software that facilitates distributed object, block, and file storage, emerged as a massively scalable storage solution for modern data pipelines to store and streamline important digital information.

To access stored data in Ceph, one can achieve this in three ways: **radosgw (RGW)**, librados, and RADOS block device (RBD). Our project focuses on Ceph's object storage, which means that the team will only be working with radosgw. Through an RGW, data can be accessed using an HTTP server with the Ceph Object Gateway daemon, which provides interfaces compatible with Amazon S3 and OpenStack Swift.

The current version of Ceph is paired with Datacenter-Data-Delivery Network (D3N), a multi-layer cache infrastructure for data centers. Its goal is to speed up performance of big data analytics by caching commonly accessed data on computing clusters connected to a larger data lake.

This project intends to build a functioning prototype of Ceph with an improved caching system: D4N. Compared to D3N, D4N introduces a Write Back cache and a Distributed Directory that allows the program to have an accurate and efficient method of locating cached data on both a cluster’s local cache or on nearby caches based on locality. D4N functions on an old research branch of Ceph, and this project is an attempt to create a Ceph Cluster with the beneficial functionality of D4N incorporated into the modern code base. The team will upstream blocks of D4N code, primarily the directory functionality, into D3N and modify the functions, classes, and pathways to properly implement the directory.

---
### Goals
The goal of this project is to incorporate D4N, an upgrade to the D3N caching architecture, in Ceph’s RADOS gateway (RGW), integrate it into the upstream open source repositories and make it available to the greater Ceph community. More specifically, this project aims to expand D4N to a global stage and synchronize D4N agents or servers into a single entity which will make the hybrid model accessible. Through a collaboration between researchers at BU, NEU and Red Hat, D4N with modified RGW servers can be distributed around the datacenter, allowing data to be cached in solid state storage (SSDs) near computer clusters to reduce load on the data center network and improve performance. This project attempts but not limited to the following:

1. Make D4N start up in vstart.sh, which is also the orchestration system in the developer workflow that is being followed.
2. Work with Red Hat and the research team to select components of D4N and rebase them on master.
3. Developing a set of unit tests for each of those components. 
4. Develop documentation and run reviews for newly introduced APIs.
5. Performance testing for different synchronization mechanisms.
6. Develop testing methodology to raise race conditions of complex distributed systems, e.g., write back while the object is being deleted - develop new interfaces to make race conditions deterministic.
7. Integrate testing into Teuthology.

Over the course of the project these goals were revised, largely in order to reduce the scope of the team. Plans to test synchronization, race conditions, and other such complex behavior was dropped as implementation goals narrowed. Instead, the team focused on the first 3 goals, as Ceph proved to be a fearsome codebase to grapple with. The modified goal for the directory side of the project became the implementation of the directory files and redis installation alongside unit testing of the setValue function for blockDirectory. BlockDirectory is the resevoir for the metadata of the smaller 4 Megabyte chunks Ceph splits larger data objects into. The final minimum goal for the directory was to integrate their work with the backend functions that would be implemented by another portion of the project's teammembers.

The backend portion of the goals narrowed signifigantly due to difficulties with porting, as will be discussed below. The minimum goal decided approximately two thirds of the way through the project became the implementation and testing of the remote cache get operation, which would allow for a rados gateway to search and fetch data from other rados gateway caches on the same cluster. This functionality was also intended to be integrated with the directory in order to provide a dynamic, vertically implemented addition to the Ceph codebase.

---
### Requirements
Virtual Machines will be used rather than containers because the latter require more overhead. Additionally, utilizing containers creates unnecessary complications in comparison to the more optimal environment that VMs offer for the purpose of this project. Finally, deploying this system on an OCT is possible but difficult to set up. Working on a VM is analogous to working on a regular host, resulting in this final environment setup.

This project requires three separate Virtual Machines (VMs). These machines have the following specifications:

- VM One serves as a gateway and is less powerful than its fellow computers. It will run with 4 cores, 8 gigabytes of RAM, and a 64 gigabyte disk.

- VMs Two and Three act as the Ceph cluster that the team will be modifying. Each VM will have 16 cores, 32 gigabytes of RAM, and 250 gigabyte disk, along with their own floating IP addresses. All three machines will be accessed through an OpenStack Terminal. As the project progressed, the third VM was modified in a way as to be nearly unusable with the python scripts the directory team was using. It was deemed easier to create a new VM than attempt to fix the current one. This VM was using the allotted resources of the one it replaced, however.

<p align="center">
	<img src="./images/LAPTOP.png" width="35%" />
</p>
<p align="center">
	<strong>Figure 1.</strong> Three VMs should be set up in total, with one acting as a gateway and the other two running the Ceph cluster.
</p>

SSDs are not necessary because system performance and development is not being tested. Instead, the local hard disk will be used to emulate the SSD and play with the network. More simply, a large file is mounted like a block device so the OSD emulates it like a hard drive and the process is automatic after the Ceph build repository is cloned and the Ceph cluster is created.

Furthermore, the team will use the current Ceph source code as a target to modify the D4N code. The D4N source code is vital to the project and has its own repository. The team’s repository for modifications, improvements, and general work is available here.

---
### Business Value
This project is intended to be used by the existing Ceph community, which consists of start up companies, research institutes, and large entities such as those that already have used the storage system. Examples of previous users are the University of Kentucky, The Minnesota Supercomputing Institute, and the United States Airforce.

Users will access Ceph through the client portion of the program and benefit from the caching upgrades of D4N through accessing metadata functions and file I/O.
	
The current cloud computing trend is an increase in the use of cache storage. Implementation of D4N will allow for larger caches that place less pressure on oversubscribed cloud networks. Furthermore, D4N is intended to improve the positioning of data in caches closer to the physical access point, saving on network bandwidth. For a program such as Ceph that is designed to scale nearly infinitely, it is key that the growing distance between clients and servers is addressed.

These are all high level, conceptual benefits to the complete integration of the D4N architeture with Ceph. The accomplishments of this team serve as a baseline to accelerate the integration by more skilled members of the Ceph community. In particular, the merging of the Redis directory was an extremely time consuming process due to the large number of files that needed to be modified in order to throughly implement the new directory files and redis integration.

---
### High-Level Design
Both D4N and D3N implementations in Ceph make heavy use of SSD or VRAM-based caching. The key limitation of D3N that this project addresses is the inability to access caches that are not part of the local computing cluster.

D4N solves this problem by introducing a directory (a Redis key store) that uses consistent value hashing to place important data in both a network cluster’s local cache and other neighboring caches. Upon a request for a data object’s location from a client, the RGW will access the directory for the data’s key and metadata, searching first through the local cache and then any nearby caches for the data. If these are all misses, then the program will access the primary data lake.

In our project, the four students will be split into two groups of two students. One team will focus on the implementation of the directory in our D3N Ceph cluster. The other group will work in parallel on the I/O side to ensure that cluster RGW’s can properly interact with nonlocal caches, the directory, and the data lake.

As noted above, D4N is already functioning on a non-upstream variant of Ceph. Since the project’s initial creation, Ceph has seen significant refactoring of classes, abstractions, and pathways. It will be up to each team to retool either the introduced D4N code or the base Ceph code to address these issues.

---
### Acceptance Criteria and Accomplished Tasks
The project's initial base goal was to implement the directory functionality from the D4N research code into our upstream Ceph cluster without signifigantly altering the existing upstream abstractions. We considered our most absolute basic goal to be implenting a 'get' function in D3N that utilizes the directory to find data stored across our Ceph network. Upon getting a get request from the client, the RGW should be able to first search its own local cache, and then query the directory in order to find if the object in remote caches, before finally searching the backend data storage.

Accomplishing this goal would have lead into the next set of objectives for the team, which was to implement read and write functionality using the D4N style directory. Implementing these two additional features with the get function is what we considered to be full completion of the project. Overall, the limited scope of our project is due to our intended goal of producing a foundation for later teams to fully integrate D4N into the upstream code. Producing solid, testable code with good practices in mind is more important than implementing as many portions of D4N as possible.

Approximately two thirds of the way through the semester it was recognised that both the implementation of the directory and the basic remote get operation were far more complex than expected. Both teams chose to ignore any potential strech goals such as the remote write functionality. The directory team focused on only getting the D4N directory to compile with D3N, with the intention of hard coding values later for testing. Similarly, the backend team decided with mentor support to focus exclusively on getting a remote cache get operation completed, using their own set of hardcoded values to negate the need for the directory.
	
Ultimately the team as a whole saw mixed success, with a definitive failure to meet the minimum viable product. The directory team was the most successful, and was able to compile ceph with the D4N directory and connect it to a functional redis key value storage. Additionally, the setValue function for cache block objects was implemented and underwent preliminary testing. The directory team did not have enough time to implement the more complex distributed system tests that were part of the initial goal, but we consider the work done by the directory to have achieved their revised goals.

The Backend team saw less direct success. The entire function stack required for remote get operations from a remote RGW was implemented in the upstream Ceph get function. However, we were unable to debug the function by the end of the project, leaving us without an end to end product. In addition, late discussions with the mentors found that some of the implemented code did not adhere to upstream Ceph's refactored namespaces, which means that the existing backend code does require more than just debugging before it can be considered an acomplishing of the minimum viable product. We consider our Acceptance Criteria not met, though we and our mentors believe we made signifigant progress in spite of that.

---
### Lessons Learned
Failure is an excellent teacher, and this project was hampered by set backs and failures. For many members of the team, this was the first time working with a very large codebase, and a very poorly commented one at that. The first hurdle, which took well over 4 weeks in total, was setting up the enviroment for Ceph. None of the team had worked on VM's either, so we were taught by our mentors how to modify our own machine's configs to ssh in the VM, how to download Ceph onto the VM's, and how to boot it up. Then, we had to learn the commands relevant to Ceph and how to quickly and efficently use the bash terminal, which was a new experience for at least one member of the team. Using the terminal well turned out to be a vitally important skill for the backend team when it came to porting, as will be discussed later. In addition, this was the first major use of git for at least one team member.

Git deserves its own moment of elaboration. It's an extremely useful tool that has a steep learning curve at first, which led to a large amount of consternation on the team. At one point in Sprint 4 the .git file was deleted on VM1, leading to about 4 days of wasted time. The reason for this deletion was an attempt by one teammember to get their pushes and commits to function properly, having never before used git in a meaningful way. After that event the whole team was more dedicated to proper git usage and managed to deal with the vast majority of issues that arose with the tool in an efficent manner. Usage of the git log and commits was vital to porting code between VM1 and VM2 as well as reverting of catastropic coding mistake.

Another batch of tools and skills learned for this project was the usage of debugging tools. The primary ones used for this project were GDB and Ceph's internally implemented log. Often times debugging is a simple process of placing printout statements throughout the code and figuring out which ones don't get reached, but that is an invalid strategy for a program as large as Ceph. Utilizing the log of Ceph is similar to printing out statements to the terminal, but required the usage of more ornate systems and was only really helpful when the statements were coupled with a relevant variable.

	
	This is just on the backend side
	1) GDB
	2) Logging
	3) git
	3) Porting code and parsing through
	4) iterative steps of improvement
	5) Terminal usage
	6) understanding project goals
	7) large program flow
	8) Reanalyzing tasks after setbacks
---
### Limitations of the Project

---

### Future for the Project
The most promising path forward for this project is the continued integration of the D4N style directory. Implementing the full setValue function for the objectDirectory is the most immediate next step, which should not prove to be a serious challenge considering the successful implementation of blockDirectory's setValue function. The next step would likely be implementing the getValue functions for both block and object metadata, and beyond that the rest of the directory's functionality should be implementable using the foundations set in this project.

The longterm goals of implenting get, read, and write functions for the write back cache, remote backends, and remote rgw caches are the ideal goal of D4N. The progress made over this project towards these goals is mixed, as the implemented code for the backend get call to a remote rgw cache is complete from D4N, but nonfunctional. It will require additional cleanup to both function at all and to fit closer into the upstream Ceph dataflow for requests. Both the teammembers and the mentors for the project have expressed interest in continuing the project in order to see this part of the D4N implementation through.

Overall, we believe that the work completed in this project is overall a benefit to the larger Ceph community, and we are proud to have helped contribute to an opensource project, a first for many of the members of the team. Though no official plans exist yet, it is likely that the team will continue the project past the end of the Fall 2021 Semester.

---
### Demos
##### **Demo 1 Video Link:** https://drive.google.com/file/d/1gHP5dZ80w4Xn2DuMSbVdR6eSDQE3XQXG/view?usp=sharing
##### **Demo 2 Video Link:** https://drive.google.com/file/d/1zKDpXfYASGvzWEACVdQuIBRUEwI-7-e-/view?usp=sharing
##### **Demo 3 Video Link:** https://drive.google.com/file/d/1NHp9ZXcn3JnY5Y3MF-aR02Azp3Jncu15/view?usp=sharing
##### **Demo 4 Video Link:** https://drive.google.com/file/d/1KU6O83Lgywgxp9eTMnjkICGFwyoUqH21/view?usp=sharing
##### **Demo 5 Video Link:** https://drive.google.com/file/d/1I4-euIcDG7HwzjdCYVZP6ByjGJuRb2hM/view?usp=sharing
---
### Sprint Breakdowns
#### Sprint 1 (September 27 to October 10)
| User Stories | Description |
| ----------- | ----------- |
| Research relevant technology and concepts | This user story involved us reading papers on Ceph and the technology it uses, such as D3N, RADOS, Redis, S3, etc to obtain a higher comprehension of what we would be working with. |
| Understand code | After meeting multiple times with our mentors and reviewing current github implementations of the codebase, we have gained a better understanding of both the D3N and D4N variations and how they are established in Ceph. |
| Environment setup | Our mentors provided us with the environment requirements that we needed to request from the professors and, after doing so, we ensured each team member had the correct permissions to access these requirements. |
| Create local Ceph cluster | We successfully cloned the Ceph github repository in both VMs and ran emulations of a Ceph cluster to ensure it has been set up properly. |
| Complete sprint 1 demo | Through various meetings with our mentors, we achieved a better understanding of the project itself as well as the work we will be focusing on throughout the course of the semester. This user story was mainly administrative, however, necessary to set up the foundation we will be building our understanding off of further along the semester, allowing us to establish a standard of organization to follow as well. We set up our project on Taiga and proceeded to record the first demonstration of the progress we have made thus far. |

#### Sprint 2 (October 11 to October 24)
| User Stories | Description |
| ----------- | ----------- |
| Set up team logistics | Our project required the workload to be split into two distinct groups. We assigned ourselves to the group we preferred and set up meeting times with our mentors to establish the next steps. |
| Understand "get" functionality | Members met with one of our mentors to work with D4N's "get" functionality and develop a deeper understanding of it. |
| Become familiarized with Ceph environment | Our mentors showed us how to navigate the Ceph environment and interact with the technology we would be working with. We practiced performing these operations on our own to ensure we were comfortable with doing so. |
| Explore D3N on Ceph VMs | We were able to learn how to put objects into, as well as get them from, the D3N cache. This allowed us to confirm the cache was working as expected and that we would be able to utilize this functionality in our future work. |
| Complete sprint 2 demo | In addition to discussing the professors' feedback for our first demo amongst ourselves and our mentors, we decided what functionality we would show and how we would format the overall video. We did not keep Taiga's representation of our work up-to-date and so we learned to ensure we do this in future sprints. |

#### Sprint 3 (October 24 to November 7)
| User Stories | Description |
| ----------- | ----------- |
| Git setup | The original repository we were working on was not forked from Ceph, which would potentially create issues in the future. As a result, I forked the original Ceph code under our  organization and pushed all the progress we have made thus far. |
| Get() directed acyclical graphs | The backend team completed DAGs for both the D3N and D4N get() functions in addition to finding the head object's data path for D4N. |
| Initial set up and understanding of directory | To set up D4N functionality in the current Ceph version built onto our VMs, we had to cherry pick D4N-related commits,  install Redis and start it up, and analyze the D4N code for a better understanding of how it works. |
| Directory porting | This user story mainly included porting the rgw_directory.cc and rgw_directory.h files into the Ceph directory and adding the former into the CMake file. |
| Backend | Like the directory porting user story, the backend team worked on porting specific D4N files to the upstreamed Ceph available on VM 1 and is working on debugging the build process. |
| Beginning of testing phase | The directory team finished their tasks for this user story, which consisted of creating a write-up with descriptions of each rgw_directory.cc function and meeting with the mentors to set up unit testing steps, practices, and the environment we would be using to test, namely, boto3. |
| GDB debugging | The backend team met with one of the mentors in order to finish the execution portion of this user story in the following sprint. |

#### Sprint 4 (November 8 to November 21)
| User Stories | Description |
| ----------- | ----------- |
| GDB debugging | The back end team learned how to use GDB to follow Ceph functions during their execution, which provided them with a deeper understanding of the relationships between the functions underlying D3N and D4N. |
| Understand D4N setup | This user story was mostly completed in sprint 3, but it was added to sprint 4 for us to fully finish. The teams familiarized themselves with the D4N environment and started keeping logs of any errors that resulted from attempting to build Ceph. |
| Resolve directory D4N porting issues | This user story included looking through the D4N files and comparing it with the analogous D3N files to add proper configurations and definitions to the upstreamed Ceph code that D4N relies on. In particular, the files rgw.yaml.in and rgw_common.h were altered and doing so allowed the radosgw binary to be built. Additionally, D4N was determined to be fully functional in VM2. |
| Make and build | The back end team is waiting on the directory team to make the ninja vstart command fully functional, as this would mean D4N has been added to Ceph. The former can then start working on I/O directing and the latter can begin testing the D4N functionalities to ensure they are present in Ceph in the next sprint. This task wasn't finished in this sprint, so it was ported over to the next one. |
| Back end | The tasks under this user story were not completed due to the corresponding team discussing their progress with the professors and being advised to reconsider how they will achieve their MVP. Instead, the goal has been changed to rethinking the path the back end team will take to move forward. |

#### Sprint 5 (November 28 to December 15)

| User Stories | Description |
| ----------- | ----------- |
| Debugging directory functionality | After porting the D4N directory functionality to upstream Ceph and adding hardcoded values to the D3N libaio write callback function, we ran into errors when attempting to get an object that was put in a bucket we made. S3 was refusing to connect, and so our mentors advised us to use GDB to look through the stack. We saw that there was a segmentation fault and it was due to our attempts to log the hardcoded values to ensure consistency. After figuring this out, our mentors provided us with next steps that allowed us to solve this issue. |
| Compilation error (continued) | This is a continuation of the previous user story with more detailed tasks that follow the process of setting up D4N and attempting to use the directory. After finishing these first two user stories, our mentors were additionally able to build off our work to dynamically update directory values rather than relying on the hardcoded values we were manually testing the directory functionality with. |
| Directory unit testing | To finish this sprint, the directory team must write unit tests that look for relevant object and block metadata in the directory and ensures it matches the actual metadata of the original objects and blocks. |
| Testing of D4N in upstream Ceph | After writing the unit tests using boto3, the directory team will use a shell script utilizing S3cmd to executing them. |
| Set up demo environment | The back end team set up a multi-RGW Ceph cluster to demonstrate the new get functionality for demo 5. |
| Porting submit get request logic | This task includes merging the back end and directory team's work and ensuring all the functionalities of D3N and D4N are kept. This user story is in process, as we are still ironing out the details of how to achieve this. |
| Hardcode get request values | This user story was created by the back end team to test their work. The tasks included hardcoding cache block and object data and modifying the RGW yaml file, which is still in progress. |

---
### Resources
1. Batra, Aman. “D4N S3 Select Caching and the Addition of Arrow Columnar Format.” YouTube, YouTube, 7 Aug. 2021, https://www.youtube.com/watch?v=X4-s978FCtM.
2. CS6620-S21. “CS6620-S21/D4N-s3select-Caching.” GitHub, https://github.com/CS6620-S21/D4N-S3Select-Caching.
3. “Current Projects.” Current Projects - MOC Documentation, https://docs.massopen.cloud/en/latest/home.html#openstack.
4. DeCandia, Giuseppe, et al. “Dynamo: Amazon’s Highly Available Key-Value Store.” All Things Distributed, Association for Computing Machinery, 14 Oct. 2007, https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf.
5. Howard, John H., et al. “Scale and Performance in a Distributed File System .” Carnegie Mellon University School of Computer Science, ACM Transactions on Computer Systems, Feb. 1988, https://www.cs.cmu.edu/~satya/docdir/howard-tocs-afs-1988.pdf. 
6. E. U. Kaynar et al., "D3N: A multi-layer cache for the rest of us," 2019 IEEE International Conference on Big Data (Big Data), 2019, pp. 327-338, doi: 10.1109/BigData47090.2019.9006396.
7. Mosayyebzadeh, Amin, et al. “D4N: Directory Based D3N.” https://docs.google.com/presentation/d/1FiEtu3biXWdSehOekR1VGr6B6AWvRtauWu0j6KL5u-I/edit#slide=id.p. 
8. “OpenStack Tutorial Index.” OpenStack Tutorial Index - MOC Documentation, https://docs.massopen.cloud/en/latest/openstack/OpenStack-Tutorial-Index.html.
9. Platz, Carol. “Ceph Storage [A Complete Explanation].” Lightbits, 3 Sept. 2021, https://www.lightbitslabs.com/blog/ceph-storage/.
10. Request a Project.” Mass Open Cloud, https://massopen.cloud/request-an-account/.
11. Weil, Sage A., et al. “Ceph: A Scalable, High-Performance Distributed File System.” Storage Systems Research Center, USENIX Association, Nov. 2006, https://www.ssrc.ucsc.edu/media/pubs/6ebbf2736ae06c66f1293b5e431082410f41f83f.pdf.
