# Kafka核心技术与实战

# 开篇

目前Apache Kafka被认为是整个消息引擎领域的执牛耳者，仅凭这一点就值得我们好好学习一下它。另外，从学习技术的角度而言，Kafka也是很有亮点的。我们仅需要学习一套框架就能在实际业务系统中实现消息引擎应用、应用程序集成、分布式存储构建，甚至是流处理应用的开发与部署，听起来还是很超值的吧。

下面是我特意为专栏画的一张思维导图，可以帮你迅速了解这个专栏的知识结构体系是什么样的。专栏大致从六个方面展开，包括Kafka入门、Kafka的基本使用、客户端详解、Kafka原理介绍、Kafka运维与监控以及高级Kafka应用。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/8b28137150c70d66200f649e26ff2395.jpg" alt="img" style="zoom: 67%;" />

- 专栏的第一部分我会介绍消息引擎这类系统大致的原理和用途，以及作为优秀消息引擎代表的Kafka在这方面的表现。
- 第二部分则重点探讨Kafka如何用于生产环境，特别是线上环境方案的制定。
- 在第三部分中我会陪你一起学习Kafka客户端的方方面面，既有生产者的实操讲解也有消费者的原理剖析，你一定不要错过。
- 第四部分会着重介绍Kafka最核心的设计原理，包括Controller的设计机制、请求处理全流程解析等。
- 第五部分则涵盖Kafka运维与监控的内容，想获得高效运维Kafka集群以及有效监控Kafka的实战经验？我必当倾囊相助！
- 最后一个部分我会简单介绍一下Kafka流处理组件Kafka Streams的实战应用，希望能让你认识一个不太一样的Kafka。



# 1、Kafka入门

## 1.1 消息引擎系统ABC

今天先来尝试回答下Kafka是什么这个问题。对了，先卖个关子，在下一期我还将继续回答这个问题，而且答案是不同的。那么，Kafka是什么呢？用一句话概括一下：

**Apache Kafka是一款开源的消息引擎系统**。

倘若“消息引擎系统”这个词对你来说有点陌生的话，那么“消息队列”“消息中间件”的提法想必你一定是有所耳闻的。不过说实话我更愿意使用消息引擎系统这个称谓，因为消息队列给出了一个很不明确的暗示，仿佛Kafka是利用队列的方式构建的；而消息中间件的提法有过度夸张“中间件”之嫌，让人搞不清楚这个中间件到底是做什么的。

**那这类系统是做什么用的呢？我先来个官方严肃版本的答案。**

根据维基百科的定义，消息引擎系统是一组规范。企业利用这组规范在不同系统之间传递语义准确的消息，实现松耦合的异步式数据传递。

果然是官方定义，有板有眼。如果觉得难于理解，那么可以试试我下面这个民间版：

系统A发送消息给消息引擎系统，系统B从消息引擎系统中读取A发送的消息。

最基础的消息引擎就是做这点事的！**不论是上面哪个版本，它们都提到了两个重要的事实：**

- 消息引擎传输的对象是消息；
- 如何传输消息属于消息引擎设计机制的一部分。

既然消息引擎是用于在不同系统之间传输消息的，**那么如何设计待传输消息的格式从来都是一等一的大事。**试问一条消息如何做到信息表达业务语义而无歧义，同时它还要能最大限度地提供可重用性以及通用性？稍微停顿几秒去思考一下，如果是你，你要如何设计你的消息编码格式。

> ​	一个比较容易想到的是使用已有的一些成熟解决方案，比如使用CSV、XML亦或是JSON；又或者你可能熟知国外大厂开源的一些序列化框架，比如Google的Protocol Buffer或Facebook的Thrift。这些都是很酷的办法。

那么现在我告诉你**Kafka的选择：它使用的是纯二进制的字节序列**。当然消息还是结构化的，只是在使用之前都要将其转换成二进制的字节序列。

消息设计出来之后还不够，消息引擎系统还要设定具体的传输协议，即我用什么方法把消息传输出去。常见的有两种方法：

- **点对点模型**：也叫消息队列模型。如果拿上面那个“民间版”的定义来说，那么系统A发送的消息只能被系统B接收，其他任何系统都不能读取A发送的消息。日常生活的例子比如电话客服就属于这种模型：同一个客户呼入电话只能被一位客服人员处理，第二个客服人员不能为该客户服务。
- **发布/订阅模型**：与上面不同的是，它有一个主题（Topic）的概念，你可以理解成逻辑语义相近的消息容器。该模型也有发送方和接收方，只不过提法不同。发送方也称为发布者（Publisher），接收方称为订阅者（Subscriber）。和点对点模型不同的是，这个模型可能存在多个发布者向相同的主题发送消息，而订阅者也可能存在多个，它们都能接收到相同主题的消息。生活中的报纸订阅就是一种典型的发布/订阅模型。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/8bc58bf5bb98db09fd6ef343e0f28826.jpg" alt="img" style="zoom: 25%;" />



## 1.2 快速搞定Kafka术语

在专栏的第一期我说过Kafka属于分布式的消息引擎系统，它的主要功能是提供一套完备的消息发布与订阅解决方案。

**在Kafka中，发布订阅的对象是主题（Topic），**你可以为每个业务、每个应用甚至是每类数据都创建专属的主题。

**生产者：**向主题发布消息的客户端应用程序称为生产者（Producer），生产者程序通常持续不断地向一个或多个主题发送消息，

**消费者：**而订阅这些主题消息的客户端应用程序就被称为消费者（Consumer）。和生产者类似，消费者也能够同时订阅多个主题的消息。

**客户端：** 我们把生产者和消费者统称为客户端（Clients）。你可以同时运行多个生产者和消费者实例，这些实例会不断地向Kafka集群中的多个主题生产和消费消息。

**服务端：**Kafka的服务器端由被称为Broker的服务进程构成，即一个Kafka集群由多个Broker组成，Broker负责接收和处理客户端发送过来的请求，以及对消息进行持久化。

> ​	虽然多个Broker进程能够运行在同一台机器上，但更常见的做法是将不同的Broker分散运行在不同的机器上，这样如果集群中某一台机器宕机，即使在它上面运行的所有Broker进程都挂掉了，其他机器上的Broker也依然能够对外提供服务。这其实就是Kafka提供高可用的手段之一。

**副本：备份机制**：实现高可用的另一个手段就是备份机制（Replication）。备份的思想很简单，就是把相同的数据拷贝到多台机器上，而这些相同的数据拷贝在Kafka中被称为副本（Replica）。

- Kafka定义了两类副本：领导者副本**（Leader Replica）**和追随者副本**（Follower Replica）**。前者对外提供服务，这里的对外指的是与客户端程序进行交互；而后者只是被动地追随领导者副本而已，不能与外界进行交互。

- 副本的工作机制也很简单：生产者总是向领导者副本写消息；而消费者总是从领导者副本读消息。至于追随者副本，它只做一件事：向领导者副本发送请求，请求领导者把最新生产的消息发给它，这样它能保持与领导者的同步。

**Scalability伸缩性：**虽然有了副本机制可以保证数据的持久化或消息不丢失，但没有解决伸缩性的问题。伸缩性即所谓的Scalability，是分布式系统中非常重要且必须要谨慎对待的问题。**什么是伸缩性呢？**我们拿副本来说，虽然现在有了领导者副本和追随者副本，但倘若领导者副本积累了太多的数据以至于单台Broker机器都无法容纳了，此时应该怎么办呢？一个很自然的想法就是，能否把数据分割成多份保存在不同的Broker上？如果你就是这么想的，那么恭喜你，Kafka就是这么设计的。

> ​	==我觉得这里不对，有问题的！！！分区的作用哪里分割数据了，==一个Broker上会保存每个主题的所有分区数据，这里应该是Leader副本如果承受不了客户端的生产消费压力了，怎么办？？？通过分区的机制起到了负载的作用！！！
>
> 为什么follower不能负责读请求呢？因为主从机制，读写分离的目的就是防止大量的读请求打在同一个服务器上造成服务器崩溃，所以mysql需要让从节点对外可以读进行读的负载均衡，然后引发了数据一致性问题。但kafka的存在这个问题吗？不，它不存在，因为分区的leader机制，一个主题的多个分区的Leader不可能在一台服务器上，而是分布在多个kafka的服务端，已经有负载均衡了。
>
> ==假如kafka一个主题三个分区，副本数量1，会是什么情况，在三台服务器会是什么情况呢？==
>
> - 因为副本数量为1，所以说此时我一个主题的数据就被分散到了不同的机器上，确实出现了这门课讲的讲一个主题的数据分散到不同的服务器上的优点；
> - **或者说，只要服务器Broker节点的数量 大于了 副本的数量，就会起到负载均衡+分割主题数据的作用**



**分区机制：保证伸缩性**：这种机制就是所谓的分区（Partitioning）；Kafka中的分区机制指的是将每个主题划分成多个分区（Partition），每个分区是一组有序的消息日志。生产者生产的每条消息只会被发送到一个分区中，也就是说如果向一个双分区的主题发送一条消息，这条消息要么在分区0中，要么在分区1中。

- 实际上，副本是在分区这个层级定义的。每个分区下可以配置若干个副本，其中只能有1个领导者副本和N-1个追随者副本。

**位移Offset：**生产者向分区写入消息，每条消息在分区中的位置信息由一个叫位移（Offset）的数据来表征。分区位移总是从0开始，假设一个生产者向一个空分区写入了10条消息，那么这10条消息的位移依次是0、1、2、…、9。

**至此我们能够完整地串联起Kafka的三层消息架构：**

- 第一层是主题层，每个主题可以配置M个分区，而每个分区又可以配置N个副本。
- 第二层是分区层，每个分区的N个副本中只能有一个充当领导者角色，对外提供服务；其他N-1个副本是追随者副本，只是提供数据冗余之用。
- 第三层是消息层，分区中包含若干条消息，每条消息的位移从0开始，依次递增。
- 最后，客户端程序只能与分区的领导者副本进行交互。

==讲完了消息层次，我们来说说Kafka Broker是如何持久化数据的；==

**消息日志（Log）：** 总的来说，Kafka使用消息日志（Log）来保存数据，一个日志就是磁盘上一个只能追加写（Append-only）消息的物理文件。因为只能追加写入，故避免了缓慢的随机I/O操作，改为性能较好的顺序I/O写操作，这也是实现Kafka高吞吐量特性的一个重要手段。

**日志段（Log Segment）机制：**不过如果你不停地向一个日志写入消息，最终也会耗尽所有的磁盘空间，因此Kafka必然要定期地删除消息以回收磁盘。怎么删除呢？简单来说就是通过日志段（Log Segment）机制。在Kafka底层，一个日志又进一步细分成多个日志段，消息被追加写到当前最新的日志段中，当写满了一个日志段后，Kafka会自动切分出一个新的日志段，并将老的日志段封存起来。Kafka在后台还有定时任务会定期地检查老的日志段是否能够被删除，从而实现回收磁盘空间的目的。

==这里再重点说说消费者。==

- 在Kafka中实现这种P2P模型的方法就是引入了**消费者组（Consumer Group）**。所谓的消费者组，指的是多个消费者实例共同组成一个组来消费一组主题

  注意主题中的每个分区只能会被同一个消费组内的一个消费者实例消费；因为假如可以被多个消费者消费，就相当于重复消费了。

  - ==对于主题的每个分区而言，它可以由多个消费组消费，但每个消费组内只有一个消费者可以消费它；==
  - ==对于消费者而言，它可以消费多个分区，可以消费一个主题的全部分区。==
  - **主题的分区 <<<====>>>消费组，他们是一一对应关系！！！消费者这个只是消费组下面打工的，随时可能跑路了。**

  

- 为什么要引入消费者组呢？**主要是为了提升消费者端的吞吐量，一个消费者消费一个主题和多个消费者消费一个主题**。多个消费者实例同时消费主题，加速整个消费端的吞吐量（TPS）。

- **Rebalance：**消费者组里面的所有消费者实例不仅“瓜分”订阅主题的数据，而且更酷的是它们还能彼此协助。假设组内某个实例挂掉了，Kafka能够自动检测到，然后把这个Failed实例之前负责的分区转移给其他活着的消费者。这个过程就是Kafka中大名鼎鼎的“重平衡”（Rebalance）。

- **消费者位移（Consumer Offset）**：每个消费者在消费消息的过程中必然需要有个字段**记录它当前消费到了分区的哪个位置上**，这个字段就是消费者位移（Consumer Offset）

  注意，这和上面所说的位移完全不是一个概念。上面的“位移”表征的是分区内的消息位置，它是不变的，即一旦消息被成功写入到一个分区上，它的位移值就是固定的了。而消费者位移则不同，它可能是随时变化的，毕竟它是消费者消费进度的指示器嘛。

  **另外每个消费者有着自己的消费者位移，**因此一定要区分这两类位移的区别。我个人把消息在分区中的位移称为分区位移，而把消费者端的位移称为消费者位移。

### 小结

我来总结一下今天提到的所有名词术语：

- 消息：Record。Kafka是消息引擎嘛，这里的消息就是指Kafka处理的主要对象。
- 主题：Topic。主题是承载消息的逻辑容器，在实际使用中多用来区分具体的业务。
- ==分区：Partition。负载均衡+分割数据量==
- 消息位移：Offset。表示分区中每条消息的位置信息，是一个单调递增且不变的值。
- ==副本：Replica。数据备份、高可用、高可靠；==
  - Kafka中同一条消息能够被拷贝到多个地方以提供数据冗余，这些地方就是所谓的副本。副本还分为领导者副本和追随者副本，各自有不同的角色划分。副本是在分区层级下的，即每个分区可配置多个副本实现高可用。
- 生产者：Producer。向主题发布新消息的应用程序。
- 消费者：Consumer。从主题订阅新消息的应用程序。
- 消费者位移：Consumer Offset。表征消费者消费进度，每个消费者都有自己的消费者位移。
- 消费者组：Consumer Group。多个消费者实例共同组成的一个组，同时消费多个分区以实现高吞吐。
- 重平衡：Rebalance。消费者组内某个消费者实例挂掉后，其他消费者实例自动重新分配订阅主题分区的过程。Rebalance是Kafka消费者端实现高可用的重要手段。

最后我用一张图来展示上面提到的这些概念，希望这张图能够帮助你形象化地理解所有这些概念：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/06dbe05a9ed4e5bcc191bbdb985352df.png" alt="img" style="zoom: 67%;" />

**为什么 Kafka 不像 MySQL 那样允许追随者副本对外提供读服务？** 

答：因为mysql一般部署在不同的机器上一台机器读写会遇到瓶颈，Kafka中的领导者副本一般均匀分布在不同的broker中，已经起到了负载的作用。即：同一个topic的已经通过分区的形式负载到不同的broker上了，读写的时候针对的领导者副本，但是量相比mysql一个还实例少太多，个人觉得没有必要在提供度读服务了。（如果量大还可以使用更多的副本，让每一个副本本身都不太大）不知道这样理解对不对?



## 1.3 Kafka只是消息引擎系统吗？

纵观Kafka的发展脉络，它的确是从消息引擎起家的，但正如文章标题所问，**Apache Kafka真的只是消息引擎吗**？通常，在回答这个问题之前很多文章可能就要这样展开了：那我们先来讨论下什么是消息引擎以及消息引擎能做什么事情。算了，我还是直给吧，就不从“唐尧虞舜”说起了。这个问题的答案是，**Apache Kafka是消息引擎系统，也是一个分布式流处理平台**（Distributed Streaming Platform）。如果你通读全篇文字但只能记住一句话，我希望你记住的就是这句。再强调一遍，Kafka是消息引擎系统，也是分布式流处理平台。



开源之后的Kafka被越来越多的公司应用到它们企业内部的数据管道中，特别是在大数据工程领域，Kafka在承接上下游、串联数据流管道方面发挥了重要的作用：所有的数据几乎都要从一个系统流入Kafka然后再流向下游的另一个系统中。这样的使用方式屡见不鲜以至于引发了Kafka社区的思考：与其我把数据从一个系统传递到下一个系统中做处理，我为何不自己实现一套流处理框架呢？基于这个考量，Kafka社区于0.10.0.0版本正式推出了流处理组件Kafka Streams，也正是从这个版本开始，Kafka正式“变身”为分布式的流处理平台，而不仅仅是消息引擎系统了。**今天Apache Kafka是和Apache Storm、Apache Spark和Apache Flink同等级的实时流处理平台。**



## 1.4 我应该选择哪种Kafka？

在专栏上一期中，我们谈了Kafka当前的定位问题，Kafka不再是一个单纯的消息引擎系统，而是能够实现精确一次（Exactly-once）处理语义的实时流处理平台。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/a2ec80dceb9ba6eeaaeebc662f439211.jpg" alt="img" style="zoom:25%;" />



## 1.5 聊聊Kafka的版本号

今天我想和你聊聊如何选择Kafka版本号这个话题。今天要讨论的内容实在是太重要了，我觉得**它甚至是你日后能否用好Kafka的关键。**

那么现在你可能会有这样的疑问：我为什么需要关心版本号的问题呢？直接使用最新版本不就好了吗？当然了，这的确是一种有效的选择版本的策略，但我想强调的是这种策略并非在任何场景下都适用。如果你不了解各个版本之间的差异和功能变化，你怎么能够准确地评判某Kafka版本是不是满足你的业务需求呢？因此在深入学习Kafka之前，花些时间搞明白版本演进，实际上是非常划算的一件事。

**Kafka版本命名**

当前Apache Kafka已经迭代到2.2版本，社区正在为2.3.0发版日期进行投票，相信2.3.0也会马上发布。但是稍微有些令人吃惊的是，很多人对于Kafka的版本命名理解存在歧义。比如我们在官网上下载Kafka时，会看到这样的版本：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/c10df9e6f72126e9c721fba38e27ac23.png" alt="img" style="zoom: 50%;" />

于是有些同学就会纳闷，难道Kafka版本号不是2.11或2.12吗？其实不然，**前面的版本号是编译Kafka源代码的Scala编译器版本。**

> ​	Kafka服务器端的代码完全由Scala语言编写，Scala同时支持面向对象编程和函数式编程，用Scala写成的源代码编译之后也是普通的“.class”文件，因此我们说Scala是JVM系的语言，它的很多设计思想都是为人称道的。

回到刚才的版本号讨论。现在你应该知道了对于kafka-2.11-2.1.1的提法，真正的Kafka版本号实际上是2.1.1。那么这个2.1.1又表示什么呢？前面的2表示大版本号，即Major Version；中间的1表示小版本号或次版本号，即Minor Version；最后的1表示修订版本号，也就是Patch号。Kafka社区在发布1.0.0版本后特意写过一篇文章，宣布Kafka版本命名规则正式从4位演进到3位，比如0.11.0.0版本就是4位版本号。

**Kafka版本演进**

Kafka目前总共演进了7个大版本，分别是0.7、0.8、0.9、0.10、0.11、1.0和2.0，其中的小版本和Patch版本很多。哪些版本引入了哪些重大的功能改进？

- 我们先从0.7版本说起，实际上也没什么可说的，这是最早开源时的“上古”版本了，以至于我也从来都没有接触过。这个版本只提供了最基础的消息队列功能，甚至连副本机制都没有，我实在想不出有什么理由你要使用这个版本，因此一旦有人向你推荐这个版本，果断走开就好了。

- Kafka从0.7时代演进到0.8之后正式引入了**副本机制**，至此Kafka成为了一个真正意义上完备的分布式高可靠消息队列解决方案。

最后我合并说下1.0和2.0版本吧，因为在我看来这两个大版本主要还是Kafka Streams的各种改进，在消息引擎方面并未引入太多的重大功能特性。Kafka Streams的确在这两个版本有着非常大的变化，也必须承认Kafka Streams目前依然还在积极地发展着。如果你是Kafka Streams的用户，至少选择2.0.0版本吧。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/9be3f8c5c2930f6482fb43d8bca507d1.jpg" alt="img" style="zoom:25%;" />







# 2、Kafka基本使用（NO）

## 2.1 线上集群部署方案

现在我们就来看看在生产环境中的Kafka集群方案该怎么做。既然是集群，那必然就要有多个Kafka节点机器，因为只有单台机器构成的Kafka伪集群只能用于日常测试之用，根本无法满足实际的线上生产需求。而真正的线上环境需要仔细地考量各种因素，结合自身的业务需求而制定。下面我就分别从操作系统、磁盘、磁盘容量和带宽等方面来讨论一下。

所谓“兵马未动，粮草先行”。与其盲目上马一套Kafka环境然后事后费力调整，不如在一开始就思考好实际场景下业务所需的集群环境。在考量部署方案时需要通盘考虑，不能仅从单个维度上进行评估。相信今天我们聊完之后，你对如何规划Kafka生产环境一定有了一个清晰的认识。现在我来总结一下今天的重点：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/d2a926bc8f448f58bda0d21b7ed39f55.jpeg" alt="img" style="zoom:50%;" />



## 2.2 集群参数配置（上）

我希望通过两期内容把这些重要的配置讲清楚。严格来说这些配置并不单单指Kafka服务器端的配置，

**其中既有Broker端参数，也有Topic级别的参数、JVM端参数、操作系统级别的参数。**

需要你注意的是，这里所说的Broker端参数也被称为静态参数（Static Configs）。我会在专栏后面介绍与静态参数相对应的动态参数。所谓静态参数，是指你必须在Kafka的配置文件server.properties中进行设置的参数，不管你是新增、修改还是删除。同时，你必须重启Broker进程才能令它们生效。而主题级别参数的设置则有所不同，Kafka提供了专门的kafka-configs命令来修改它们。至于JVM和操作系统级别参数，它们的设置方法比较通用化，我介绍的也都是标准的配置参数，因此，你应该很容易就能够对它们进行设置。



目前Kafka Broker提供了近200个参数，这其中绝大部分参数都不用你亲自过问。

**首先Broker是需要配置存储信息的，**即Broker使用哪些磁盘。那么针对存储信息的重要参数有以下这么几个：

- `log.dirs`：这是非常重要的参数，指定了Broker需要使用的若干个文件目录路径。要知道这个参数是没有默认值的，这说明什么？这说明它必须由你亲自指定。
- `log.dir`：注意这是dir，结尾没有s，说明它只能表示单个路径，它是补充上一个参数用的。

> ​	这两个参数应该怎么设置呢？很简单，你只要设置`log.dirs`，即第一个参数就好了，不要设置`log.dir`。而且更重要的是，在线上生产环境中一定要为`log.dirs`配置多个路径，具体格式是一个CSV格式，也就是用逗号分隔的多个路径，比如`/home/kafka1,/home/kafka2,/home/kafka3`这样。如果有条件的话你最好保证这些目录挂载到不同的物理磁盘上。这样做有两个好处：
>
> - 提升读写性能：比起单块磁盘，多块物理磁盘同时读写数据有更高的吞吐量。
> - 能够实现故障转移：即Failover。这是Kafka 1.1版本新引入的强大功能。要知道在以前，只要Kafka Broker使用的任何一块磁盘挂掉了，整个Broker进程都会关闭。但是自1.1开始，这种情况被修正了，坏掉的磁盘上的数据会自动地转移到其他正常的磁盘上，而且Broker还能正常工作。还记得上一期我们关于Kafka是否需要使用RAID的讨论吗？这个改进正是我们舍弃RAID方案的基础：没有这种Failover的话，我们只能依靠RAID来提供保障。

**下面说说与ZooKeeper相关的设置。**首先ZooKeeper是做什么的呢？

- 它是一个分布式协调框架，负责协调管理并保存Kafka集群的所有元数据信息，比如集群都有哪些Broker在运行、创建了哪些Topic，每个Topic都有多少分区以及这些分区的Leader副本都在哪些机器上等信息。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/d5248ba158a2283c095324a265d9f7e7.jpg" alt="img" style="zoom:25%;" />

## 2.3 集群参数配置（下）

下半部分主要是Topic级别参数、JVM参数以及操作系统参数的设置。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/da521c645594bcf0e4670a3d20937b87.jpg" alt="img" style="zoom:25%;" />







# 3、客户端实践及原理



## 3.1 消息分区机制

我们在使用Apache Kafka生产和消费消息的时候，肯定是希望能够将数据均匀地分配到所有服务器上。比如很多公司使用Kafka收集应用服务器的日志数据，这种数据都是很多的，特别是对于那种大批量机器组成的集群环境，每分钟产生的日志量都能以GB数，因此如何将这么大的数据量均匀地分配到Kafka的各个Broker上，就成为一个非常重要的问题。



### 3.1.1 为什么分区？

专栏前面我说过Kafka有主题（Topic）的概念，它是承载真实数据的逻辑容器，而在主题之下还分为若干个分区，也就是说Kafka的消息组织方式实际上是三级结构：**主题-分区-消息。**

主题下的每条消息只会保存在某一个分区中，而不会在多个分区中被保存多份。官网上的这张图非常清晰地展 示了Kafka的三级结构，如下所示：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/18e487b7e64eeb8d0a487c289d83ab63.png" alt="img" style="zoom: 67%;" />

现在我抛出一个问题：为什么Kafka要做这样的设计？为什么使用分区的概念而不是直接使用多个主题呢？

==其实分区的作用就是提供负载均衡的能力，==或者说对数据进行分区的主要原因，就是为了实现系统的高伸缩性（Scalability）。不同的分区能够被放置到不同节点的机器上，而数据的读写操作也都是针对分区这个粒度而进行的，这样每个节点的机器都能独立地执行各自分区的读写请求处理。并且，我们还可以通过添加新的节点机器来增加整体系统的吞吐量。

==假如kafka一个主题三个分区，副本数量1，会是什么情况，在三台服务器会是什么情况呢？==

- 因为副本数量为1，所以说此时我一个主题的数据就被分散到了不同的机器上，确实出现了这门课讲的讲一个主题的数据分散到不同的服务器上的优点；
- 或者说，只要服务器Broker节点的数量 大于了 副本的数量，就会起到负载均衡+分割主题数据的作用



### 3.1.2 都有哪些分区策略？

下面我们说说Kafka生产者的分区策略。**所谓分区策略是决定生产者将消息发送到哪个分区的算法。**Kafka为我们提供了默认的分区策略，同时它也支持你自定义分区策略。

**如果要自定义分区策略，**你需要显式地配置生产者端的参数`partitioner.class`。方法很简单，在编写生产者程序时，你可以编写一个具体的类实现`org.apache.kafka.clients.producer.Partitioner`接口。

> ​	这个接口也很简单，只定义了两个方法：`partition()`和`close()`，通常你只需要实现最重要的partition方法。我们来看看这个方法的方法签名：
>
> ```
> int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
> ```
>
> 这里的`topic`、`key`、`keyBytes`、`value`和`valueBytes`都属于消息数据，`cluster`则是集群信息（比如当前Kafka集群共有多少主题、多少Broker等）。Kafka给你这么多信息，就是希望让你能够充分地利用这些信息对消息进行分区，计算出它要被发送到哪个分区中。只要你自己的实现类定义好了partition方法，同时设置`partitioner.class`参数为你自己实现类的Full Qualified Name，那么生产者程序就会按照你的代码逻辑对消息进行分区。

**轮询策略**

也称Round-robin策略，即顺序分配。比如一个主题下有3个分区，那么第一条消息被发送到分区0，第二条被发送到分区1，第三条被发送到分区2，以此类推。当生产第4条消息时又会重新开始，即将其分配到分区0，就像下面这张图展示的那样。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/6c630aaf0b365115897231a4e0a7e1af.png" alt="img" style="zoom: 33%;" />

轮询策略有非常优秀的负载均衡表现，它总是能保证消息最大限度地被平均分配到所有分区上，故默认情况下它是最合理的分区策略，也是我们最常用的分区策略之一。

**随机策略**

也称Randomness策略。所谓随机就是我们随意地将消息放置到任意一个分区上，如下面这张图所示。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/5b50b76efb8ada0f0779ac3275d215a3.png" alt="img" style="zoom: 33%;" />

机策略是老版本生产者使用的分区策略，在新版本中已经改为轮询了。



## 3.2 无消息丢失配置

Kafka到底在什么情况下才能保证消息不丢失呢？

**一句话概括，Kafka只对“已提交”的消息（committed message）做有限度的持久化保证。**

- 第一个核心要素是“**已提交的消息**”。什么是已提交的消息？当Kafka的若干个Broker成功地接收到一条消息并写入到日志文件后，它们会告诉生产者程序这条消息已成功提交。此时，这条消息在Kafka看来就正式变为“已提交”消息了。

> ​	那为什么是若干个Broker呢？这取决于你对“已提交”的定义。你可以选择只要有一个Broker成功保存该消息就算是已提交，也可以是令所有Broker都成功保存该消息才算是已提交。不论哪种情况，Kafka只对已提交的消息做持久化保证这件事情是不变的。

- 第二个核心要素就是“**有限度的持久化保证**”，也就是说Kafka不可能保证在任何情况下都做到不丢失消息。

  > 举个极端点的例子，如果地球都不存在了，Kafka还能保存任何消息吗？显然不能！倘若这种情况下你依然还想要Kafka不丢消息，那么只能在别的星球部署Kafka Broker服务器了。现在你应该能够稍微体会出这里的“有限度”的含义了吧，其实就是说Kafka不丢消息是有前提条件的。假如你的消息保存在N个Kafka Broker上，那么这个前提条件就是这N个Broker中至少有1个存活。只要这个条件成立，Kafka就能保证你的这条消息永远不会丢失。



### 3.2.1 “消息丢失”案例

好了，理解了Kafka是怎样做到不丢失消息的，那接下来我带你复盘一下那些**常见的“Kafka消息丢失”案例**。注意，这里可是带引号的消息丢失哦，其实有些时候我们只是冤枉了Kafka而已。

**案例1：生产者程序丢失数据**

Producer程序丢失消息，这应该算是被抱怨最多的数据丢失场景了。

> 我来描述一个场景：你写了一个Producer应用向Kafka发送消息，最后发现Kafka没有保存，于是大骂：“Kafka真烂，消息发送居然都能丢失，而且还不告诉我？！”如果你有过这样的经历，那么请先消消气，我们来分析下可能的原因。

目前Kafka Producer是异步发送消息的，也就是说如果你调用的是producer.send(msg)这个API，那么它通常会立即返回，但此时你不能认为消息发送已成功完成。

如果用这个方式，可能会有哪些因素导致消息没有发送成功呢？其实原因有很多，例如网络抖动，导致消息压根就没有发送到Broker端；或者消息本身不合格导致Broker拒绝接收（比如消息太大了，超过了Broker的承受能力）等。这么来看，让Kafka“背锅”就有点冤枉它了。就像前面说过的，Kafka不认为消息是已提交的，因此也就没有Kafka丢失消息这一说了。

**Producer永远要使用带有回调通知的发送API，也就是说不要使用producer.send(msg)，而要使用producer.send(msg, callback)**。不要小瞧这里的callback（回调），它能准确地告诉你消息是否真的提交成功了。一旦出现消息提交失败的情况，你就可以有针对性地进行处理。

> 你可能会问，**发送失败真的没可能是由Broker端的问题造成的吗？**当然可能！如果你所有的Broker都宕机了，那么无论Producer端怎么重试都会失败的，此时你要做的是赶快处理Broker端的问题。但之前说的核心论据在这里依然是成立的：**Kafka依然不认为这条消息属于已提交消息**，故对它不做任何持久化保证。

**案例2：消费者程序丢失数据**

Consumer端丢失数据主要体现在Consumer端要消费的消息不见了。Consumer程序有个“位移”的概念，表示的是这个Consumer当前消费到的Topic分区的位置。下面这张图来自于官网，它清晰地展示了Consumer端的位移数据。比如对于Consumer A而言，它当前的位移值就是9；Consumer B的位移值是11。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/0c97bed3b6350d73a9403d9448290d37.png" alt="img" style="zoom: 25%;" />

办法很简单：**维持先消费消息，再更新位移的顺序**即可。这样就能最大限度地保证消息不丢失。

其次：**处理消费消息，Consumer程序不要开启自动提交位移，而是要应用程序手动提交位移**。（自动提交位移是在poll下来消息后，就马上提交了，但此时消息可能没有被消费成功，如服务器异常了）

==当然，这种处理方式可能带来的问题是**消息的重复处理**，==但这不属于消息丢失的情形。在专栏后面的内容中，我会跟你分享如何应对重复消费的问题。

==所以可以看到：Kafka在不丢失消息 和 消息的重复处理 两者只能保证一个==，而消息的重复处理我们可以使用外部手段来保证，不丢失消息无法使用外部手段，所以我们会通过kafka自身保证不丢失消息设置，kafka不管消息的重复处理。



### 3.2.2 **最佳实践**

看完这两个案例之后，我来分享一下Kafka无消息丢失的配置，每一个其实都能对应上面提到的问题。

1. **使用同步发送producer.send(msg)，不要使用异步发送producer.send(msg, callback)。**

   <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/image-20230721144930964.png" alt="image-20230721144930964" style="zoom:50%;" />

   <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/image-20230721150841048.png" alt="image-20230721150841048" style="zoom:80%;" />

   <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/image-20230721145708371.png" alt="image-20230721145708371" style="zoom: 50%;" />

   ​			异步发送，生产者发送完消息后就可以执行后续业务，不会有任何阻塞，Broker端在收到消息写入后会异步调用生产者提供的callback回调方法。吞吐量很大但是会丢消息。

   

2. **设置acks = all 或1，依靠情况而定（仅对同步发送有效）**。acks是Producer的一个参数，代表了你对“已提交”消息的定义。如果设置成all，则表明所有副本Broker都要接收到消息，该消息才算是“已提交”。这是最高等级的“已提交”定义。

   - `acks=0`：表示消息发送之后Broker会立即返回ack确认信息，无论消息是否写成功
   - `acks=1`：表示消息发送后并写成功kafka的topic对应分区的leader节点就返回成功
   - `acks=all`：表示消息发送并写成功topic对应分区的leader节点，所有副本follow来同步数据成功，返回给leader节点，leader节点在一起返回确认给生产侧才表示写成功

3. **设置重试次数retries为一个较大的值。**这里的retries同样是Producer的参数，对应前面提到的Producer自动重试。

4. **设置unclean.leader.election.enable = false。**这是Broker端的参数，它控制的是哪些Broker有资格竞选分区的Leader。如果一个Broker落后原先的Leader太多，那么它一旦成为新的Leader，必然会造成消息的丢失。故一般都要将该参数设置成false，即不允许这种情况的发生。

5. **设置副本数量replication.factor >= 3。**这也是Broker端的参数。其实这里想表述的是，最好将消息多保存几份。

6. **设置min.insync.replicas > 1。**这依然是Broker端参数，控制的是消息至少要被写入到多少个副本才算是“已提交”。和前面的第二点中acks=all配置是相辅相成的作用

7. **副本数量replication.factor > min.insync.replicas**。如果两者相等，那么只要有一个副本挂机，整个分区就无法正常工作了。我们不仅要改善消息的持久性，防止数据丢失，还要在不降低可用性的基础上完成。推荐设置成replication.factor = min.insync.replicas + 1。

8. **Consumer**端有个参数enable.auto.commit，最好把它设置成false。

   

   



## 3.3 客户端拦截器功能

既然是不常见，那就说明在实际场景中并没有太高的出场率，但它们依然是很高级很实用的。下面就有请今天的主角登场：Kafka拦截器。

### 3.3.1**什么是拦截器？**

如果你用过Spring Interceptor或是Apache Flume，那么应该不会对拦截器这个概念感到陌生，其基本思想就是允许应用程序在不修改逻辑的情况下，动态地实现一组可插拔的事件处理逻辑链。它能够在主业务操作的前后多个时间点上插入对应的“拦截”逻辑。下面这张图展示了Spring MVC拦截器的工作原理：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/096831a3ba037b3f9e507e6db631d3c4.png" alt="img" style="zoom:67%;" />

拦截器1和拦截器2分别在请求发送之前、发送之后以及完成之后三个地方插入了对应的处理逻辑。而Flume中的拦截器也是同理，它们插入的逻辑可以是修改待发送的消息，也可以是创建新的消息，甚至是丢弃消息。这些功能都是以配置拦截器类的方式动态插入到应用程序中的，故可以快速地切换不同的拦截器而不影响主程序逻辑。

Kafka拦截器借鉴了这样的设计思路。你可以在消息处理的前后多个时点动态植入不同的处理逻辑，比如在消息发送前或者在消息被消费后。

作为一个非常小众的功能，Kafka拦截器自0.10.0.0版本被引入后并未得到太多的实际应用，我也从未在任何Kafka技术峰会上看到有公司分享其使用拦截器的成功案例。但即便如此，在自己的Kafka工具箱中放入这么一个有用的东西依然是值得的。今天我们就让它来发挥威力，展示一些非常酷炫的功能。

### 3.3.2**Kafka拦截器**

**Kafka拦截器分为生产者拦截器和消费者拦截器**。生产者拦截器允许你在发送消息前以及消息提交成功后植入你的拦截器逻辑；而消费者拦截器支持在消费消息前以及提交位移后编写特定逻辑。值得一提的是，这两种拦截器都支持链的方式，即你可以将一组拦截器串连成一个大的拦截器，Kafka会按照添加顺序依次执行拦截器逻辑。

举个例子，假设你想在生产消息前执行两个“前置动作”：第一个是为消息增加一个头信息，封装发送该消息的时间，第二个是更新发送消息数字段，那么当你将这两个拦截器串联在一起统一指定给Producer后，Producer会按顺序执行上面的动作，然后再发送消息。

当前Kafka拦截器的设置方法是通过参数配置完成的。生产者和消费者两端有一个相同的参数，名字叫interceptor.classes，它指定的是一组类的列表，每个类就是特定逻辑的拦截器实现类。拿上面的例子来说，假设第一个拦截器的完整类路径是com.yourcompany.kafkaproject.interceptors.AddTimeStampInterceptor，第二个类是com.yourcompany.kafkaproject.interceptors.UpdateCounterInterceptor，那么你需要按照以下方法在Producer端指定拦截器：

```
Properties props = new Properties();
List interceptors = new ArrayList<>();
interceptors.add("com.yourcompany.kafkaproject.interceptors.AddTimestampInterceptor"); // 拦截器1
interceptors.add("com.yourcompany.kafkaproject.interceptors.UpdateCounterInterceptor"); // 拦截器2
props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
……
```

现在问题来了，我们应该怎么编写AddTimeStampInterceptor和UpdateCounterInterceptor类呢？其实很简单，这两个类以及你自己编写的所有Producer端拦截器实现类都要继承org.apache.kafka.clients.producer.ProducerInterceptor接口。该接口是Kafka提供的，里面有两个核心的方法。

1. onSend：该方法会在消息发送之前被调用。如果你想在发送之前对消息“美美容”，这个方法是你唯一的机会。
2. onAcknowledgement：该方法会在消息成功提交或发送失败之后被调用。还记得我在上一期中提到的发送回调通知callback吗？onAcknowledgement的调用要早于callback的调用。值得注意的是，这个方法和onSend不是在同一个线程中被调用的，因此如果你在这两个方法中调用了某个共享可变对象，一定要保证线程安全哦。还有一点很重要，这个方法处在Producer发送的主路径中，所以最好别放一些太重的逻辑进去，否则你会发现你的Producer TPS直线下降。

同理，指定消费者拦截器也是同样的方法，只是具体的实现类要实现org.apache.kafka.clients.consumer.ConsumerInterceptor接口，这里面也有两个核心方法。

1. onConsume：该方法在消息返回给Consumer程序之前调用。也就是说在开始正式处理消息之前，拦截器会先拦一道，搞一些事情，之后再返回给你。
2. onCommit：Consumer在提交位移之后调用该方法。通常你可以在该方法中做一些记账类的动作，比如打日志等。

一定要注意的是，**指定拦截器类时要指定它们的全限定名**，即full qualified name。通俗点说就是要把完整包名也加上，不要只有一个类名在那里，并且还要保证你的Producer程序能够正确加载你的拦截器类。

### 3.3.3**典型使用场景**

Kafka拦截器都能用在哪些地方呢？其实，跟很多拦截器的用法相同，**Kafka拦截器可以应用于包括客户端监控、端到端系统性能检测、消息审计等多种功能在内的场景**。

我以端到端系统性能检测和消息审计为例来展开介绍下。

今天Kafka默认提供的监控指标都是针对单个客户端或Broker的，你很难从具体的消息维度去追踪集群间消息的流转路径。同时，如何监控一条消息从生产到最后消费的端到端延时也是很多Kafka用户迫切需要解决的问题。

从技术上来说，我们可以在客户端程序中增加这样的统计逻辑，但是对于那些将Kafka作为企业级基础架构的公司来说，在应用代码中编写统一的监控逻辑其实是很难的，毕竟这东西非常灵活，不太可能提前确定好所有的计算逻辑。另外，将监控逻辑与主业务逻辑耦合也是软件工程中不提倡的做法。

现在，通过实现拦截器的逻辑以及可插拔的机制，我们能够快速地观测、验证以及监控集群间的客户端性能指标，特别是能够从具体的消息层面上去收集这些数据。这就是Kafka拦截器的一个非常典型的使用场景。

我们再来看看消息审计（message audit）的场景。设想你的公司把Kafka作为一个私有云消息引擎平台向全公司提供服务，这必然要涉及多租户以及消息审计的功能。

作为私有云的PaaS提供方，你肯定要能够随时查看每条消息是哪个业务方在什么时间发布的，之后又被哪些业务方在什么时刻消费。一个可行的做法就是你编写一个拦截器类，实现相应的消息审计逻辑，然后强行规定所有接入你的Kafka服务的客户端程序必须设置该拦截器。

### 3.3.4**案例分享**

下面我以一个具体的案例来说明一下拦截器的使用。在这个案例中，我们通过编写拦截器类来统计消息端到端处理的延时，非常实用，我建议你可以直接移植到你自己的生产环境中。

我曾经给一个公司做Kafka培训，在培训过程中，那个公司的人提出了一个诉求。他们的场景很简单，某个业务只有一个Producer和一个Consumer，他们想知道该业务消息从被生产出来到最后被消费的平均总时长是多少，但是目前Kafka并没有提供这种端到端的延时统计。

学习了拦截器之后，我们现在知道可以用拦截器来满足这个需求。既然是要计算总延时，那么一定要有个公共的地方来保存它，并且这个公共的地方还是要让生产者和消费者程序都能访问的。在这个例子中，我们假设数据被保存在Redis中。

Okay，这个需求显然要实现生产者拦截器，也要实现消费者拦截器。我们先来实现前者：

```
public class AvgLatencyProducerInterceptor implements ProducerInterceptor {


    private Jedis jedis; // 省略Jedis初始化


    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        jedis.incr("totalSentMessage");
        return record;
    }


    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }


    @Override
    public void close() {
    }


    @Override
    public void configure(Map configs) {
    }
```

上面的代码比较关键的是在发送消息前更新总的已发送消息数。为了节省时间，我没有考虑发送失败的情况，因为发送失败可能导致总发送数不准确。不过好在处理思路是相同的，你可以有针对性地调整下代码逻辑。

下面是消费者端的拦截器实现，代码如下：

```
public class AvgLatencyConsumerInterceptor implements ConsumerInterceptor {


    private Jedis jedis; //省略Jedis初始化


    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        long lantency = 0L;
        for (ConsumerRecord record : records) {
            lantency += (System.currentTimeMillis() - record.timestamp());
        }
        jedis.incrBy("totalLatency", lantency);
        long totalLatency = Long.parseLong(jedis.get("totalLatency"));
        long totalSentMsgs = Long.parseLong(jedis.get("totalSentMessage"));
        jedis.set("avgLatency", String.valueOf(totalLatency / totalSentMsgs));
        return records;
    }


    @Override
    public void onCommit(Map offsets) {
    }


    @Override
    public void close() {
    }


    @Override
    public void configure(Map configs) {
```

在上面的消费者拦截器中，我们在真正消费一批消息前首先更新了它们的总延时，方法就是用当前的时钟时间减去封装在消息中的创建时间，然后累计得到这批消息总的端到端处理延时并更新到Redis中。之后的逻辑就很简单了，我们分别从Redis中读取更新过的总延时和总消息数，两者相除即得到端到端消息的平均处理延时。

创建好生产者和消费者拦截器后，我们按照上面指定的方法分别将它们配置到各自的Producer和Consumer程序中，这样就能计算消息从Producer端到Consumer端平均的处理延时了。这种端到端的指标监控能够从全局角度俯察和审视业务运行情况，及时查看业务是否满足端到端的SLA目标。

### 3.3.5**小结**

今天我们花了一些时间讨论Kafka提供的冷门功能：拦截器。如之前所说，拦截器的出场率极低，以至于我从未看到过国内大厂实际应用Kafka拦截器的报道。但冷门不代表没用。事实上，我们可以利用拦截器满足实际的需求，比如端到端系统性能检测、消息审计等。

从这一期开始，我们将逐渐接触到更多的实际代码。看完了今天的分享，我希望你能够亲自动手编写一些代码，去实现一个拦截器，体会一下Kafka拦截器的功能。要知道，“纸上得来终觉浅，绝知此事要躬行”。也许你在敲代码的同时，就会想到一个使用拦截器的绝妙点子，让我们拭目以待吧。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/f2cbe18428396aab14749be10dc5550d.jpg" alt="img" style="zoom:25%;" />



## 3.4 Java生产者的TCP连接管理

​			Kafka的Java生产者是如何管理TCP连接的。



### 3.4.1 为何采用TCP？

Apache Kafka的所有通信都是基于TCP的，而不是基于HTTP或其他协议。无论是生产者、消费者，还是Broker之间的通信都是如此。

你可能会问，为什么Kafka不使用HTTP作为底层的通信协议呢？其实这里面的原因有很多，但最主要的原因在于TCP和HTTP之间的区别。

- **在开发客户端时，人们能够利用TCP本身提供的一些高级功能，比如多路复用请求以及同时轮询多个连接的能力;**
- **目前已知的HTTP库在很多编程语言中都略显简陋**



### 3.4.2 Kafka生产者程序概览

Kafka的Java生产者API主要的对象就是KafkaProducer。通常我们开发一个生产者的步骤有4步。

第1步：构造生产者对象所需的参数对象。

第2步：利用第1步的参数对象，创建KafkaProducer对象实例。

第3步：使用KafkaProducer的send方法发送消息。

第4步：调用KafkaProducer的close方法关闭生产者并释放各种系统资源。

上面这4步写成Java代码的话大概是这个样子：

```
Properties props = new Properties ();
props.put(“参数1”, “参数1的值”)；
props.put(“参数2”, “参数2的值”)；
……
try (Producer producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord(……), callback);
	……
}
```

现在问题来了，当我们开发一个Producer应用时，生产者会向Kafka集群中指定的主题（Topic）发送消息，这必然涉及与Kafka Broker创建TCP连接。那么，Kafka的Producer客户端是如何管理这些TCP连接的呢？



### 3.4.3 何时创建TCP连接？

要回答上面这个问题，我们首先要弄明白生产者代码是什么时候创建TCP连接的。就上面的那段代码而言，可能创建TCP连接的地方有两处：Producer producer = new KafkaProducer(props)和producer.send(msg, callback)。

==首先，生产者应用在创建KafkaProducer实例时会建立与Broker的TCP连接。==其实这种表述也不是很准确，应该这样说：在创建KafkaProducer实例时，**生产者应用会在后台创建并启动一个名为Sender的线程，该Sender线程开始运行时首先会创建与Broker的连接。**

那如果不调用send方法，这个Producer都不知道给哪个主题发消息，它又怎么能知道连接哪个Broker呢？答案是：它会连接bootstrap.servers参数指定的所有Broker！！！这就稍微有点麻烦了，所以不要把集群中所有的Broker信息都配置到bootstrap.servers中，通常你指定3～4台就足以了。

> 我在这里稍微解释一下bootstrap.servers参数。**它是Producer的核心参数之一，指定了这个Producer启动时要连接的Broker地址。**请注意，这里的“启动时”，代表的是Producer启动时会发起与这些Broker的连接。因此，如果你为这个参数指定了1000个Broker连接信息，那么很遗憾，你的Producer启动时会首先创建与这1000个Broker的TCP连接。
>
> 在实际使用过程中，我并不建议把集群中所有的Broker信息都配置到bootstrap.servers中，通常你指定3～4台就足以了。因为Producer一旦连接到集群中的任一台Broker，就能拿到整个集群的Broker信息，故没必要为bootstrap.servers指定所有的Broker。

==其次还有两个地方可能会存在producer和broker之间创建连接，一个是在更新元数据后，另一个是在消息发送时。==

**为什么说是可能？因为这两个地方并非总是创建TCP连接**。当Producer更新了集群的元数据信息之后，如果发现与某些Broker当前没有连接，那么它就会创建一个TCP连接。同样地，当要发送消息时，Producer发现尚不存在与目标Broker的连接，也会创建一个。

> 接下来，我们来看看Producer更新集群元数据信息的两个场景。
>
> 场景一：当Producer尝试给一个不存在的主题发送消息时，Broker会告诉Producer说这个主题不存在。此时Producer会发送METADATA请求给Kafka集群，去尝试获取最新的元数据信息。
>
> 场景二：Producer通过metadata.max.age.ms参数定期地去更新元数据信息。该参数的默认值是300000，即5分钟，也就是说不管集群那边是否有变化，Producer每5分钟都会强制刷新一次元数据以保证它是最及时的数据。





## 3.4 如何保证消息精准一次（不用看）

今天我要和你分享的主题是：Kafka消息交付可靠性保障以及精确处理一次语义的实现；

所谓的消息交付可靠性保障，是指Kafka对Producer和Consumer要处理的消息提供什么样的承诺。常见的承诺有以下三种：

- 最多消费一次（at most once）：消息可能会丢失，但绝不会被重复发送。
- 至少消费一次（at least once）：消息不会丢失，但有可能被重复发送。
- 精确消费一次（exactly once）：消息不会丢失，也不会被重复发送。

目前，Kafka默认提供的交付可靠性保障是第二种，即至少一次。

**那么Kafka是怎么做到精确消费一次的呢？**

- 简单来说通过两种机制：幂等性（Idempotence）和事务（Transaction）



### 3.4.1 幂等性Producer—单分区单会话

在Kafka中，Producer默认不是幂等性的，但我们可以创建幂等性Producer。（它其实是0.11.0.0版本引入的新功能。）

- **对于非幂等型的Producer，**Kafka向分区发送数据时，可能会出现同一条消息被发送了多次，导致消息重复的情况。
- ==**而幂等性的Producer，**虽然也会重复向Broker端发消息，但Broker端会帮忙对消息去重；==

> ​	指定Producer幂等性的方法很简单，仅需要设置一个参数即可，即`props.put(“enable.idempotence”, ture)`，或`props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG， true)`;设置完之后，Producer自动升级成幂等性Producer，其他所有的代码逻辑都不需要改变。Kafka自动帮你做消息的重复去重。

**底层具体的原理很简单就是在Broker端多保存一些字段**（经典的用空间去换时间）。当Producer发送了具有相同字段值的消息后，Broker能够自动知晓这些消息已经重复了，于是可以在后台默默地把它们“丢弃”掉。当然，实际的实现原理并没有这么简单，但你大致可以这么理解。

==幂等性Producer的作用范围：==

- **1、它只能保证单分区上的幂等性，**即一个幂等性Producer能够保证某个主题的分区上不出现重复消息，它无法实现多个分区的幂等性。

  因为我的去重复机制是依赖Broker端的信息啊，Broker端只知道每个分区的情况撒

- **2、它只能实现单会话上的幂等性，**不能实现跨会话的幂等性。这里的会话，你可以理解为Producer进程的一次运行。

  当你重启了Producer进程之后，这种幂等性保证就丧失了。





### 3.4.2 事务型Producer

- 事务型Producer能够保证将所有消息原子性地写入到多个分区中。这批消息要么全部写入成功，要么全部失败。

- 另外，事务型Producer也不惧进程的重启。Producer重启回来后，Kafka依然保证它们发送消息的精确一次处理。



设置事务型Producer的方法也很简单，满足两个要求即可：

- 和幂等性Producer一样，开启enable.idempotence = true。
- 设置Producer端参数transactional. id。最好为其设置一个有意义的名字。

此外，你还需要在Producer代码中做一些调整，如这段代码所示：

```
producer.initTransactions();
try {
            producer.beginTransaction();
            producer.send(record1);
            producer.send(record2);
            producer.commitTransaction();
} catch (KafkaException e) {
            producer.abortTransaction();
}
```

和普通Producer代码相比，事务型Producer的显著特点是调用了一些事务API，如initTransaction、beginTransaction、commitTransaction和abortTransaction，它们分别对应事务的初始化、事务开始、事务提交以及事务终止。

这段代码能够保证Record1和Record2被当作一个事务统一提交到Kafka，要么它们全部提交成功，要么全部写入失败。实际上即使写入失败，Kafka也会把它们写入到底层的日志中，也就是说Consumer还是会看到这些消息。**因此在Consumer端，读取事务型Producer发送的消息也是需要一些变更的。**

修改起来也很简单，设置isolation.level参数的值即可。当前这个参数有两个取值：

1. read_uncommitted：这是默认值，表明Consumer能够读取到Kafka写入的任何消息，不论事务型Producer提交事务还是终止事务，其写入的消息都可以读取。很显然，如果你用了事务型Producer，那么对应的Consumer就不要使用这个值。
2. read_committed：表明Consumer只会读取事务型Producer成功提交事务写入的消息。当然了，它也能看到非事务型Producer写入的所有消息。



### 小结

简单来说，幂等性Producer和事务型Producer都是Kafka社区力图为Kafka实现精确一次处理语义所提供的工具，只是它们的作用范围是不同的。幂等性Producer只能保证单分区、单会话上的消息幂等性；而事务能够保证跨分区、跨会话间的幂等性。从交付语义上来看，自然是事务型Producer能做的更多。

不过，切记天下没有免费的午餐。比起幂等性Producer，事务型Producer的性能要更差，在实际使用过程中，我们需要仔细评估引入事务的开销，切不可无脑地启用事务。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/419a092ef55d0fa248a56fe582a551ed.jpg" alt="img" style="zoom:24%;" />

## 3.5 消费者组到底是什么？

==那么何谓Consumer Group呢？==

用一句话概括就是：**Consumer Group是Kafka提供的可扩展且具有容错性的消费者机制**。

- Group ID：既然是一个组，那么组内必然可以有**多个消费者Consumer Instance），它们共享一个公共的ID，**这个ID被称为Group ID。
- 组内的所有消费者协调在一起来消费订阅主题（Subscribed Topics）的所有分区（Partition）

个人认为，理解Consumer Group记住下面这三个特性就好了：

1. Consumer Group下可以有一个或多个Consumer实例。这里的实例可以是一个单独的进程，也可以是同一进程下的线程。在实际场景中，使用进程更为常见一些。
2. Group ID是一个字符串，在一个Kafka集群中，它标识唯一的一个Consumer Group。
3. Consumer Group下所有实例订阅的主题的单个分区，只能分配给组内的某个Consumer实例消费。这个分区当然也可以被其他的Group消费；

**理想情况下，Consumer实例的数量应该等于该Group订阅主题的分区总数。**因为设置多余的实例只会浪费资源，而没有任何好处。

==消费组的位移管理==

好了，说完了Consumer Group的设计特性，我们来讨论一个问题：**针对Consumer Group，Kafka是怎么管理位移的呢？**你还记得吧，消费者在消费的过程中需要记录自己消费了多少数据，即消费位置信息。在Kafka中，这个位置信息有个专门的术语：**位移（Offset）**。看上去该Offset就是一个数值而已，其实对于Consumer Group而言，**它是一组KV对，Key是分区，V对应该消费组-消费该分区的最新位移。**

> 我在专栏[第4期](https://time.geekbang.org/column/article/100285)中提到过Kafka有新旧客户端API之分，那自然也就有新旧Consumer之分。老版本的Consumer也有消费者组的概念，它和我们目前讨论的Consumer Group在使用感上并没有太多的不同，只是它管理位移的方式和新版本是不一样的。
>
> 老版本的Consumer Group把位移保存在ZooKeeper中。Apache ZooKeeper是一个分布式的协调服务框架，Kafka重度依赖它实现各种各样的协调管理。将位移保存在ZooKeeper外部系统的做法，最显而易见的好处就是减少了Kafka Broker端的状态保存开销。现在比较流行的提法是将服务器节点做成无状态的，这样可以自由地扩缩容，实现超强的伸缩性。Kafka最开始也是基于这样的考虑，才将Consumer Group位移保存在独立于Kafka集群之外的框架中。
>
> 不过，慢慢地人们发现了一个问题，即ZooKeeper这类元框架其实并不适合进行频繁的写更新，而Consumer Group的位移更新却是一个非常频繁的操作。这种大吞吐量的写操作会极大地拖慢ZooKeeper集群的性能，因此Kafka社区渐渐有了这样的共识：将Consumer位移保存在ZooKeeper中是不合适的做法。
>
> 于是，在新版本的Consumer Group中，Kafka社区重新设计了Consumer Group的位移管理方式，采用了将位移保存在Kafka内部主题的方法。

Kafka将消费组的位移管理保存在Kafka内部主题：consumer_offsets。

==消费组的rebalance机制==

> 最后，我们来说说**Consumer Group端大名鼎鼎的重平衡，**也就是所谓的Rebalance过程。我形容其为“大名鼎鼎”，从某种程度上来说其实也是“臭名昭著”，因为有关它的bug真可谓是此起彼伏，从未间断。这里我先卖个关子，后面我会解释它“遭人恨”的地方。

**1、什么是Rebalance？**

- Rebalance本质上是一种协议，规定了一个Consumer Group下的所有Consumer如何达成一致，来分配订阅Topic的每个分区。比如某个Group下有20个Consumer实例，它订阅了一个具有100个分区的Topic。正常情况下，Kafka平均会为每个Consumer分配5个分区。这个分配的过程就叫Rebalance。

**2、Consumer Group何时进行Rebalance？**

Rebalance的触发条件有3个。

​	1、组成员数发生变更。比如有新的Consumer实例加入组或者离开组，抑或是有Consumer实例崩溃被“踢出”组。

​	2、订阅主题数发生变更。Consumer Group可以使用正则表达式的方式订阅主题，比如consumer.subscribe(Pattern.compile(“t.*c”))就表明该Group订阅所有以字母t开头、字母c结尾的主题。在Consumer Group的运行过程中，你新创建了一个满足这样条件的主题，那么该Group就会发生Rebalance。

​	3、分区数发生变更。Kafka当前只能允许增加一个主题的分区数。当分区数增加时，就会触发订阅该主题的所有Group开启Rebalance。

**3、Rebalance发生时，Group下所有的Consumer实例都会协调在一起共同参与。**

当前Kafka默认提供了3种分配策略，每种策略都有一定的优势和劣势，我们今天就不展开讨论了，你只需要记住社区会不断地完善这些策略，保证提供最公平的分配策略，即每个Consumer实例都能够得到较为平均的分区数。

**4、Rebalance的缺点：类似JVM里面的STW。**

​	1、Rebalance影响Consumer端TPS。首先，Rebalance过程中会发生“STW”，在STW期间，所有Consumer实例都会停止消费，等待Rebalance完成。这是Rebalance为人诟病的一个方面。

​	2、Rebalance效率不高。目前Rebalance的设计是所有Consumer实例共同参与，全部重新分配所有分区。其实更高效的做法是尽量减少分配方案的变动。

> 例如实例A之前负责消费分区1、2、3，那么Rebalance之后，如果可能的话，最好还是让实例A继续消费分区1、2、3，而不是被重新分配其他的分区。这样的话，实例A连接这些分区所在Broker的TCP连接就可以继续用，不用重新创建连接其他Broker的Socket资源。

​	3、最后，Rebalance实在是太慢了。

> 曾经，有个国外用户的Group内有几百个Consumer实例，成功Rebalance一次要几个小时！这完全是不能忍受的。最悲剧的是，目前社区对此无能为力，至少现在还没有特别好的解决方案。所谓“本事大不如不摊上”，也许最好的解决方案就是避免Rebalance的发生吧。



总结一下，今天我跟你分享了Kafka Consumer Group的方方面面，包括它是怎么定义的，它解决了哪些问题，有哪些特性。同时，我们也聊到了Consumer Group的位移管理以及著名的Rebalance过程。希望在你开发Consumer应用时，它们能够助你一臂之力。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/60478ddbf101b19a747d8110ae019ef5.jpg" alt="img" style="zoom:25%;" />



## 3.6 位移主题

==consumer_offsets：位移主题==

Consumer的位移管理机制其实也很简单，就是将消费组的位移数据作为一条条普通的Kafka消息，提交到consumer_offsets中。

> ​	这里我想再次强调一下，和你创建的其他主题一样，位移主题就是普通的Kafka主题。你可以手动地创建它、修改它，甚至是删除它。只不过，它同时也是一个内部主题，大部分情况下，你其实并不需要“搭理”它，也不用花心思去管理它，把它丢给Kafka就完事了。

虽说位移主题是一个普通的Kafka主题，但它的消息格式却是Kafka自己定义的，用户不能修改，也就是说你不能随意地向这个主题写消息，因为一旦你写入的消息不满足Kafka规定的格式，那么Kafka内部无法成功解析，就会造成Broker的崩溃。

你可能会好奇，这个主题存的到底是什么格式的消息呢？所谓的消息格式，你可以简单地理解为是一个KV对。

- 位移主题的Key中应该保存3部分内容：**<Group ID，主题名，分区号>**。

- 消息体可以简单的认为保存一个位移值就可以了，虽然实际情况更加复杂；

==那么位移主题是怎么被创建的呢？==

通常来说，**当Kafka集群中的第一个Consumer程序启动时，Kafka会自动创建位移主题**。我们说过，位移主题就是普通的Kafka主题，那么它自然也有对应的分区数。但如果是Kafka自动创建的，分区数是怎么设置的呢？这就要看Broker端参数`offsets.topic.num.partitions`的取值了。它的默认值是50，因此Kafka会自动创建一个50分区的位移主题。如果你曾经惊讶于Kafka日志路径下冒出很多__consumer_offsets-xxx这样的目录，那么现在应该明白了吧，这就是Kafka自动帮你创建的位移主题啊。

你可能会问，除了分区数，副本数或备份因子是怎么控制的呢？答案也很简单，这就是Broker端另一个参数`offsets.topic.replication.factor`要做的事情了。它的默认值是3。

==那Consumer是怎么提交位移的呢？==

目前Kafka Consumer提交位移的方式有两种：**自动提交位移和手动提交位移。**

==Kafka是怎么删除位移主题中的过期消息的呢？==

因为不断的写入位移消息，总有一天会写满磁盘的。Kafka使用**Compact策略**来删除位移主题中的过期消息，避免该主题无限期膨胀。

**Kafka提供了专门的后台线程定期地巡检待Compact的主题，看看是否存在满足条件的可删除数据**。这个后台线程叫Log Cleaner。很多实际生产环境中都出现过位移主题无限膨胀占用过多磁盘空间的问题，如果你的环境中也有这个问题，我建议你去检查一下Log Cleaner线程的状态，通常都是这个线程挂掉了导致的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/927e436fb8054665d81db418c25af3b7.jpg" alt="img" style="zoom:25%;" />



## 3.7 Coordinator&Rebalance

==rebalance的负责人：Coordinator==

Rebalance就是让一个Consumer Group下所有的Consumer实例就如何消费订阅主题的所有分区达成共识的过程。在Rebalance过程中，所有Consumer实例共同参与，在**协调者组件的帮助下**，完成订阅主题分区的分配。但是，在整个过程中，所有实例都不能消费任何消息，因此它对Consumer的TPS影响很大。

**所谓协调者，在Kafka中对应的术语是Coordinator，**它专门为Consumer Group服务，负责为Group执行Rebalance以及提供位移管理和组成员管理等。

- 具体来讲，Consumer端应用程序在提交位移时，其实是向Coordinator所在的Broker提交位移。同样地，当Consumer应用启动时，也是向Coordinator所在的Broker发送各种请求，然后由Coordinator负责执行消费者组的注册、成员管理记录等元数据管理操作。

**所有Broker都有各自的Coordinator组件**。所有Broker在启动时，都会创建和开启相应的Coordinator组件。

==那么Consumer Group如何确定为它服务的Coordinator在哪台Broker上呢？==

答案就在Kafka内部位移主题__consumer_offsets身上，**负责管理该消费组位移主题的分区的Leader副本**

目前，Kafka为某个Consumer Group确定Coordinator所在的Broker的算法有2个步骤：

- 第1步：确定由位移主题的哪个分区来保存该Group数据：partitionId=Math.abs(groupId.hashCode() % offsetsTopicPartitionCount)。

- 第2步：找出该分区Leader副本所在的Broker，该Broker即为对应的Coordinator。

在实际使用过程中，Consumer应用程序，特别是Java Consumer API，能够自动发现并连接正确的Coordinator，我们不用操心这个问题。知晓这个算法的最大意义在于，它能够帮助我们解决定位问题。当Consumer Group出现问题，需要快速排查Broker端日志时，我们能够根据这个算法准确定位Coordinator对应的Broker，不必一台Broker一台Broker地盲查。

==下面我们就来说说如何避免Rebalance。==

要避免Rebalance，还是要从Rebalance发生的时机入手。

- **组成员数量发生变化**
- 订阅主题数量发生变化（运维不可避免）
- 订阅主题的分区数发生变化（运维不可避免）

后面两个通常都是运维的主动操作，所以它们引发的Rebalance大都是不可避免的。接下来，我们主要说说因为组成员数量变化而引发的Rebalance该如何避免。如果Consumer Group下的Consumer实例数量发生变化，就一定会引发Rebalance。这是Rebalance发生的最常见的原因。我碰到的99%的Rebalance，都是这个原因导致的。

**1、增加Consumer；**

Consumer实例增加的情况很好理解，当我们启动一个配置有相同group.id值的Consumer程序时，实际上就向这个Group添加了一个新的Consumer实例。此时，Coordinator会接纳这个新实例，将其加入到组中，并重新分配分区。通常来说，增加Consumer实例的操作都是计划内的，可能是出于增加TPS或提高伸缩性的需要。总之，它不属于我们要规避的那类“不必要Rebalance”。

**2、减少Consumer；**

如果你就是要停掉某些Consumer实例，那自不必说，关键是在某些情况下，**Consumer实例会被Coordinator错误地认为“已停止”从而被“踢出”Group**。如果是这个原因导致的Rebalance，我们就不能不管了。

==Coordinator会在什么情况下认为某个Consumer实例已挂从而要退组呢？==

- **心跳请求：**当Consumer Group完成Rebalance之后，每个Consumer实例都会定期地向Coordinator发送心跳请求，表明它还存活着。如果没有及时发送心跳请求，Coordinator就会认为该Consumer已经“死”了，从而将其从Group中移除，然后开启新一轮Rebalance。

  > ​	**Consumer端有个参数，可以理解为心跳超时时间，**叫session.timeout.ms，就是被用来表征此事的。该参数的默认值是10秒，即如果Coordinator在10秒之内没有收到Group下某Consumer实例的心跳，它就会认为这个Consumer实例已经挂了。可以这么说，session.timeout.ms决定了Consumer存活性的时间间隔。

  > ​	**Consumer还提供了控制发送心跳请求频率的参数**，就是heartbeat.interval.ms，因为可能因为网络原因有些心跳无法正常送达Broker。这个值设置得越小，Consumer实例发送心跳请求的频率就越高。频繁地发送心跳请求会额外消耗带宽资源，但好处是能够更加快速地知晓当前是否开启Rebalance，

- **消费能力评估：**max.poll.interval.ms参数：限定了Consumer端应用程序两次调用poll方法的最大时间间隔。

  如果consumer在这个时间内无法完成两次拉取数据，就说明该消费者实例消费能力太差，Coordinator需要把它踢出去，否则容易造成消息积压。

==搞清楚了这些参数的含义，接下来我们来明确一下到底哪些Rebalance是“不必要的”。==

- **第一类非必要Rebalance是因为未能及时发送心跳，导致Consumer被“踢出”Group而引发的**。因此，你需要仔细地设置session.timeout.ms和heartbeat.interval.ms的值。我在这里给出一些推荐数值，你可以“无脑”地应用在你的生产环境中。
  
  - 设置session.timeout.ms = 6s。
  - 设置heartbeat.interval.ms = 2s。
  - 要保证Consumer实例在被判定为“dead”之前，能够发送至少3轮的心跳请求，即session.timeout.ms >= 3 * heartbeat.interval.ms。
  
- **第二类非必要Rebalance是Consumer消费时间过长导致的**。

  我之前有一个客户，在他们的场景中，Consumer消费数据时需要将消息处理之后写入到MongoDB。显然，这是一个很重的消费逻辑。MongoDB的一丁点不稳定都会导致Consumer程序消费时长的增加。此时，**max.poll.interval.ms**参数值的设置显得尤为关键。如果要避免非预期的Rebalance，你最好将该参数值设置得大一点，比你的下游最大处理时间稍长一点。

==如果你按照上面的推荐数值恰当地设置了这几个参数，却发现还是出现了Rebalance！==

那么我建议你去排查一下**Consumer端的GC表现**，比如是否出现了频繁的Full GC导致的长时间停顿，从而引发了Rebalance。为什么特意说GC？那是因为在实际场景中，我见过太多因为GC设置不合理导致程序频发Full GC而引发的非预期Rebalance了。



<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/321c73b51f5e5c3124765101edc53ed3.jpg" alt="img" style="zoom:25%;" />



## 3.8 提交位移&重复消费

这里主要是关于Consumer端如何将已经消费的消息位移信息发给Broker端的位移主题进行保存，对此针对的问题是消费的重复消费和消息丢失的问题；

**Consumer的消费位移，它记录了Consumer要消费的下一条消息的位移（或是说位置）。**例如假设一个分区中有10条消息，位移分别是0到9。某个Consumer应用已消费了5条消息，这就说明该Consumer消费了位移为0到4的5条消息，此时Consumer的位移是5，指向了下一条消息的位移。

**Consumer需要向Kafka汇报自己的位移数据，这个汇报过程被称为提交位移**（Committing Offsets）。

因为Consumer能够同时消费多个分区的数据，==所以位移的提交实际上是在分区粒度上进行的==；

==自动提交和手动提交：==

- 自动提交位移：每隔一段时间自动向Broker提交当前我的消费位移；

  **可能发生消息还没有消费完的情况，旧提交了位移导致消息丢失**

  Consumer端有个参数enable.auto.commit，默认值就是true，即Java Consumer默认就是自动提交位移的。如果启用了自动提交，Consumer端还有个参数就派上用场了：auto.commit.interval.ms。它的默认值是5秒，表明Kafka每5秒会为你自动提交一次位移。

  

- 手动提交位移：自己要在代码中使用Kafka提供的API进行提交位移；

  开启手动提交位移先enable.auto.commit为false；

  - **手动同步提交API：KafkaConsumer#commitSync()**，它是一个同步阻塞操作，即该方法会一直等待，直到远端的Broker返回提交结果，这个状态才会结束如果提交过程中出现异常，该方法会将异常信息抛出。在任何系统中，因为程序而非资源限制而导致的阻塞都可能是系统的瓶颈，会影响整个应用程序的TPS。

    ```java
    while (true) {
                ConsumerRecords records =
                            consumer.poll(Duration.ofSeconds(1));
                process(records); // 处理消息
                try {
                            consumer.commitSync();
                } catch (CommitFailedException e) {
                            handle(e); // 处理提交失败异常
                }
    }
    ```

  - **手动异步提交API：KafkaConsumer#commitAsync()**，是一个异步操作。调用commitAsync()之后，它会立即返回，不会阻塞，因此不会影响Consumer应用的TPS，基于回调机制。

    ```java
    while (true) {
                ConsumerRecords records = 
    	consumer.poll(Duration.ofSeconds(1));
                process(records); // 处理消息
                consumer.commitAsync((offsets, exception) -> {
    	if (exception != null)
    	handle(exception);
    	});
    }
    ```

==commitAsync是否能够替代commitSync呢？答案是不能！异步的问题在于，出现问题时它不会自动重试。==

因为它是异步操作，倘若提交失败后自动重试，那么它重试时提交的位移值可能早已经“过期”或不是最新值了。因此，异步提交的重试其实没有意义，所以commitAsync是不会重试的。



**显然，我们需要将commitSync和commitAsync组合使用才能到达最理想的效果**，原因有两个：

1. 我们可以利用commitSync的自动重试来规避那些瞬时错误，比如网络的瞬时抖动，Broker端GC等。因为这些问题都是短暂的，自动重试通常都会成功，因此，我们不想自己重试，而是希望Kafka Consumer帮我们做这件事。
2. 我们不希望程序总处于阻塞状态，影响TPS。

我们来看一下下面这段代码，它展示的是如何将两个API方法结合使用进行手动提交。只要最后一次提交位移成功，那么就可以保证Broker端存储的消费位移是最新的。

```java
   try {
           while(true) {
                        ConsumerRecords records = consumer.poll(Duration.ofSeconds(1));
                        process(records); // 处理消息
                        commitAysnc(); // 使用异步提交规避阻塞
            }
} catch(Exception e) {
            handle(e); // 处理异常
} finally {
            try {
                        consumer.commitSync(); // 最后一次提交使用同步阻塞式提交
	} finally {
	     consumer.close();
}
}
```

==上述方法解决了消息丢失的问题，即确保为消费者一定消费了这条消息，然后才去提交消费位移，==

**但是重复消费的问题没有得到解决**

> 刚刚我们聊到的所有位移提交，都是提交poll方法返回的所有消息的位移，比如poll方法一次返回了500条消息，当你处理完这500条消息之后，前面我们提到的各种方法会一次性地将这500条消息的位移一并处理。简单来说，就是**直接提交最新一条消息的位移**。但如果我想更加细粒度化地提交位移，该怎么办呢？
>
> **Kafka中每条poll下来的消息都携带着自己在分区中的位移信息。**在每次消费者调用poll()方法获取消息时，Kafka会返回一个记录集（RecordSet），它包含了一批消息记录。每个消息记录都有自己的位移信息，表示消息在对应分区中的位置。位移记录在消息记录中的位置信息通过offset()方法获取。

例如，我消费500条，消费到了200出现异常就停止了，都还没有轮到提交位移那一步，这样在Broker端依然是之前的500前面的消费位移，下一次poll时还是拉的上次500，就会重复消费了这200条数据；

==外部手段解决重复消费问题==

- redis实现分布式锁：每条消息给一个分布式id，消费者只能一条条消费，每次消费之前先去获取该锁，锁的key就是消息的分布式ID，获取到了就消费，否则就不消费该消息；
- 依靠外部数据库，将分布式id存到数据库，每次查一下该id是否已经存在了，如果存在就不消费跳过它；

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/a6e24c364321aaa44b8fedf3836bccd1.jpg" alt="img" style="zoom:25%;" />



## 3.9 CommitFailedException异常

**所谓CommitFailedException，顾名思义就是Consumer在提交位移时出现了错误或异常，而且还是那种不可恢复的严重异常**。

我们来看一下这个异常的一些介绍：

> ​	和CommitFailedException一起出现的，还有一段非常著名的注释。为什么说它很“著名”呢？第一，我想不出在近50万行的Kafka源代码中，还有哪个异常类能有这种待遇，可以享有这么大段的注释，来阐述其异常的含义；第二，纵然有这么长的文字解释，却依然有很多人对该异常想表达的含义感到困惑。
>
> > Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.
> >
> > 这段话前半部分的意思是，本次提交位移失败了，原因是消费者组已经开启了Rebalance过程，并且将要提交位移的分区分配给了另一个消费者实例。

==出现这个情况的原因是，你的消费者实例调用poll方法的时间间隔超过了期望的max.poll.interval.ms参数值。==

==超出了之后，Coordinator就会把这个能力差的消费者提出组，立马进行rebalance，此时提交位移就会出现异常==。

当消息处理的总时间超过预设的max.poll.interval.ms参数值时，Kafka Consumer端会抛出CommitFailedException异常。这是该异常最“正宗”的登场方式。你只需要写一个Consumer程序，使用KafkaConsumer.subscribe方法随意订阅一个主题，之后设置Consumer端参数max.poll.interval.ms=5秒，最后在循环调用KafkaConsumer.poll方法之间，插入Thread.sleep(6000)和手动提交位移，就可以成功复现这个异常了。在这里，我展示一下主要的代码逻辑。

```
…
Properties props = new Properties();
…
props.put("max.poll.interval.ms", 5000);
consumer.subscribe(Arrays.asList("test-topic"));
 
while (true) {
    ConsumerRecords records = 
		consumer.poll(Duration.ofSeconds(1));
    // 使用Thread.sleep模拟真实的消息处理逻辑
    Thread.sleep(6000L);
    consumer.commitSync();
}
```

==如果要防止这种场景下抛出异常，就是加强消费者消费能力==。

具体来说有4种方法：

1. **缩短单条消息处理的时间**。比如，之前下游系统消费一条消息的时间是100毫秒，优化之后成功地下降到50毫秒；
2. **加大max.poll.interval.ms配置值**。如果你的消费逻辑不能简化，那么提高该参数值是一个不错的办法，可能引发消息挤压。
3. **降低每次poll的消息数量**。这个依据你的下游消费端能力而言，但可能引发消息积压。
4. **下游系统使用多线程来加速消费**。这个可以，充分利用机器性能，不会引发消息挤压





## 3.11 Java 消费者的TCP连接管理

前面专门聊过“Java生产者是如何管理TCP连接资源的”这个话题，下面我们一起来研究下Kafka的Java消费者管理TCP或Socket资源的机制。只有完成了今天的讨论，我们才算是对Kafka客户端的TCP连接管理机制有了全面的了解。

和之前一样，我今天会无差别地混用TCP和Socket两个术语。毕竟，在Kafka的世界中，无论是ServerSocket，还是SocketChannel，它们实现的都是TCP协议。或者这么说，Kafka的网络传输是基于TCP协议的，而不是基于UDP协议，因此，当我今天说到TCP连接或Socket资源时，我指的是同一个东西。



### 3.11.1 何时创建TCP连接？

**和生产者不同的是，构建KafkaConsumer实例时是不会创建任何TCP连接的**。

> 生产者入口类KafkaProducer在构建实例的时候，会在后台默默地启动一个Sender线程，这个Sender线程负责Socket连接的创建。
>
> 从这一点上来看，我个人认为KafkaConsumer的设计比KafkaProducer要好。就像我在第13讲中所说的，在Java构造函数中启动线程，会造成this指针的逃逸，这始终是一个隐患。

==TCP连接是在调用KafkaConsumer.poll方法时被创建的==。

再细粒度地说，在poll方法内部有3个时机可以创建TCP连接。

**1、发起FindCoordinator请求时**

还记得消费者端有个组件叫协调者（Coordinator）吗？它驻留在Broker端的内存中，负责消费者组的组成员管理和各个消费者的位移提交管理。当消费者程序首次启动调用poll方法时，它需要向Kafka集群发送一个名为FindCoordinator的请求，希望Kafka集群告诉它哪个Broker是管理它的协调者。

不过，消费者应该向哪个Broker发送这类请求呢？理论上任何一个Broker都能回答这个问题，也就是说消费者可以发送FindCoordinator请求给集群中的任意服务器。

**2、连接协调者Coordinator时**

Broker处理完上一步发送的FindCoordinator请求之后，会返还对应的响应结果（Response），显式地告诉消费者哪个Broker是真正的协调者，因此在这一步，消费者知晓了真正的协调者后，会创建连向该Broker的Socket连接。只有成功连入协调者，协调者才能开启正常的组协调操作，比如加入组、等待组分配方案、心跳请求处理、位移获取、位移提交等。

**3、消费数据时**

消费者会创建自己消费分区Leader副本所在Broker连接的TCP。举个例子，假设消费者要消费5个分区的数据，这5个分区各自的领导者副本分布在4台Broker上，那么该消费者在消费时会创建与这4台Broker的Socket连接。



### 3.11.2 创建多少个TCP连接？

下面我们来说说消费者创建TCP连接的数量。你可以先思考一下大致需要的连接数量，然后我们结合具体的Kafka日志，来验证下结果是否和你想的一致。

我们来看看这段日志。

> *[2019-05-27 10:00:54,142] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9092 (id: -1 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944)*

> *…*

> *[2019-05-27 10:00:54,188] DEBUG [Consumer clientId=consumer-1, groupId=test] Sending metadata request MetadataRequestData(topics=[MetadataRequestTopic(name=‘t4’)], allowAutoTopicCreation=true, includeClusterAuthorizedOperations=false, includeTopicAuthorizedOperations=false) to node localhost:9092 (id: -1 rack: null) (org.apache.kafka.clients.NetworkClient:1097)*

> *…*

> *[2019-05-27 10:00:54,188] TRACE [Consumer clientId=consumer-1, groupId=test] Sending FIND_COORDINATOR {key=test,key_type=0} with correlation id 0 to node -1 (org.apache.kafka.clients.NetworkClient:496)*

> *[2019-05-27 10:00:54,203] TRACE [Consumer clientId=consumer-1, groupId=test] Completed receive from node -1 for FIND_COORDINATOR with correlation id 0, received {throttle_time_ms=0,error_code=0,error_message=null, node_id=2,host=localhost,port=9094} (org.apache.kafka.clients.NetworkClient:837)*

> *…*

> *[2019-05-27 10:00:54,204] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9094 (id: 2147483645 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944)*

> *…*

> *[2019-05-27 10:00:54,237] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9094 (id: 2 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944)*

> *[2019-05-27 10:00:54,237] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9092 (id: 0 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944)*

> *[2019-05-27 10:00:54,238] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9093 (id: 1 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944)*

这里我稍微解释一下，日志的第一行是消费者程序创建的第一个TCP连接，就像我们前面说的，这个Socket用于发送FindCoordinator请求。由于这是消费者程序创建的第一个连接，此时消费者对于要连接的Kafka集群一无所知，因此它连接的Broker节点的ID是-1，表示消费者根本不知道要连接的Kafka Broker的任何信息。

值得注意的是日志的第二行，消费者复用了刚才创建的那个Socket连接，向Kafka集群发送元数据请求以获取整个集群的信息。

日志的第三行表明，消费者程序开始发送FindCoordinator请求给第一步中连接的Broker，即localhost:9092，也就是nodeId等于-1的那个。在十几毫秒之后，消费者程序成功地获悉协调者所在的Broker信息，也就是第四行标为橙色的“node_id = 2”。

完成这些之后，消费者就已经知道协调者Broker的连接信息了，因此在日志的第五行发起了第二个Socket连接，创建了连向localhost:9094的TCP。只有连接了协调者，消费者进程才能正常地开启消费者组的各种功能以及后续的消息消费。

在日志的最后三行中，消费者又分别创建了新的TCP连接，主要用于实际的消息获取。还记得我刚才说的吗？要消费的分区的领导者副本在哪台Broker上，消费者就要创建连向哪台Broker的TCP。在我举的这个例子中，localhost:9092，localhost:9093和localhost:9094这3台Broker上都有要消费的分区，因此消费者创建了3个TCP连接。

看完这段日志，你应该会发现日志中的这些Broker节点的ID在不断变化。有时候是-1，有时候是2147483645，只有在最后的时候才回归正常值0、1和2。这又是怎么回事呢？

前面我们说过了-1的来由，即消费者程序（其实也不光是消费者，生产者也是这样的机制）首次启动时，对Kafka集群一无所知，因此用-1来表示尚未获取到Broker数据。

那么2147483645是怎么来的呢？它是由Integer.MAX_VALUE减去协调者所在Broker的真实ID计算得来的。看第四行标为橙色的内容，我们可以知道协调者ID是2，因此这个Socket连接的节点ID就是Integer.MAX_VALUE减去2，即2147483647减去2，也就是2147483645。这种节点ID的标记方式是Kafka社区特意为之的结果，目的就是要让组协调请求和真正的数据获取请求使用不同的Socket连接。

至于后面的0、1、2，那就很好解释了。它们表征了真实的Broker ID，也就是我们在server.properties中配置的broker.id值。

**我们来简单总结一下上面的内容。通常来说，消费者程序会创建3类TCP连接：**

1. **确定协调者和获取集群元数据。**
2. **连接协调者，令其执行组成员管理操作。**
3. **执行实际的消息获取。**

**那么，这三类TCP请求的生命周期都是相同的吗？换句话说，这些TCP连接是何时被关闭的呢？**



### 3.11.3 何时关闭TCP连接？

和生产者类似，消费者关闭Socket也分为主动关闭和Kafka自动关闭。

- 主动关闭是指你显式地调用消费者API的方法去关闭消费者，具体方式就是**手动调用KafkaConsumer.close()方法，或者是执行Kill命令**，不论是Kill -2还是Kill -9；
- 而Kafka自动关闭是由**消费者端参数connection.max.idle.ms**控制的，该参数现在的默认值是9分钟，即如果某个Socket连接上连续9分钟都没有任何请求“过境”的话，那么消费者会强行“杀掉”这个Socket连接。

针对上面提到的三类TCP连接，你需要注意的是，**当第三类TCP连接成功创建后，消费者程序就会废弃第一类TCP连接**，之后在定期请求元数据时，它会改为使用第三类TCP连接。也就是说，最终你会发现，第一类TCP连接会在后台被默默地关闭掉。对一个运行了一段时间的消费者程序来说，只会有后面两类TCP连接存在。

### 小结

好了，今天我们补齐了Kafka Java客户端管理TCP连接的“拼图”。我们不仅详细描述了Java消费者是怎么创建和关闭TCP连接的，还对目前的设计方案提出了一些自己的思考。希望今后你能将这些知识应用到自己的业务场景中，并对实际生产环境中的Socket管理做到心中有数。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/f13d7008d7b251df0e6e6a89077d7604.jpg" alt="img" style="zoom:25%;" />





## 3.12 消费者组消费进度监控



### 3.12.1 Kafka自带命令

使用Kafka自带的命令行工具bin/kafka-consumer-groups.sh(bat)。

**kafka-consumer-groups脚本是Kafka为我们提供的最直接的监控消费者消费进度的工具**。当然，除了监控Lag之外，它还有其他的功能。今天，我们主要讨论如何使用它来监控Lag。

使用kafka-consumer-groups脚本很简单。该脚本位于Kafka安装目录的bin子目录下，我们可以通过下面的命令来查看某个给定消费者的Lag值：

```
$ bin/kafka-consumer-groups.sh --bootstrap-server  --describe --group 
```

**Kafka连接信息就是<主机名：端口>对，而group名称就是你的消费者程序中设置的group.id值**。我举个实际的例子来说明具体的用法，请看下面这张图的输出：

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/18bc0ee629cfa761b1d17e638be9f67d.png)

在运行命令时，指定了Kafka集群的连接信息localhost:9092。设置了要查询的消费者组名：testgroup。

**kafka-consumer-groups脚本的输出信息很丰富。**

- 首先，它会按照消费者组订阅主题的分区进行展示，每个分区一行数据；

- 其次，除了主题、分区等信息外，它会汇报每个分区当前最新生产的消息的位移值（即LOG-END-OFFSET列值）、该消费者组当前最新消费消息的位移值（即CURRENT-OFFSET值）、LAG值（前两者的差值）、消费者实例ID、消费者连接Broker的主机名以及消费者的CLIENT-ID信息。



### 3.12.2 Kafka Java Consumer API

社区提供的Java Consumer API分别提供了**查询当前分区最新消息位移**和**消费者组最新消费消息位移**两组方法，我们使用它们就能计算出对应的Lag。



### 3.12.3 Kafka JMX监控指标

上面这两种方式，都可以很方便地查询到给定消费者组的Lag信息。但在很多实际监控场景中，我们借助的往往是现成的监控框架。如果是这种情况，以上这两种办法就不怎么管用了，因为它们都不能集成进已有的监控框架中，如Zabbix或Grafana。

下面我们就来看第三种方法，使用Kafka默认提供的JMX监控指标来监控消费者的Lag值。

当前，Kafka消费者提供了一个名为

**kafka.consumer:type=consumer-fetch-manager-metrics,client-id=“{client-id}”**

的JMX指标，里面有很多属性。和我们今天所讲内容相关的有两组属性：**records-lag-max和records-lead-min**，它们分别表示此消费者在测试窗口时间内曾经达到的最大的Lag值和最小的Lead值。

Lag值的含义我们已经反复讲过了，我就不再重复了。**这里的Lead值是指消费者最新消费消息的位移与分区当前第一条消息位移的差值**。很显然，Lag和Lead是一体的两个方面：**Lag越大的话，Lead就越小，反之也是同理**。

你可能会问，为什么要引入Lead呢？我只监控Lag不就行了吗？这里提Lead的原因就在于这部分功能是我实现的。开个玩笑，其实社区引入Lead的原因是，==只看Lag的话，我们也许不能及时意识到可能出现的严重问题==。

试想一下，监控到Lag越来越大，可能只会给你一个感受，那就是消费者程序变得越来越慢了，至少是追不上生产者程序了，除此之外，你可能什么都不会做。毕竟，有时候这也是能够接受的。但反过来，一旦你监测到Lead越来越小，甚至是快接近于0了，你就一定要小心了，**这可能预示着消费者端要丢消息了。**

> ​	为什么？我们知道Kafka的消息是有留存时间设置的，默认是1周，也就是说Kafka默认删除1周前的数据。倘若你的消费者程序足够慢，慢到它要消费的数据快被Kafka删除了，这时你就必须立即处理，否则一定会出现消息被删除，从而导致消费者程序重新调整位移值的情形。这可能产生两个后果：一个是消费者从头消费一遍数据，另一个是消费者从最新的消息位移处开始消费，之前没来得及消费的消息全部被跳过了，从而造成丢消息的假象。

这两种情形都是不可忍受的，因此必须有一个JMX指标，清晰地表征这种情形，这就是引入Lead指标的原因。

所以，Lag值从100万增加到200万这件事情，远不如Lead值从200减少到100这件事来得重要。**在实际生产环境中，请你一定要同时监控Lag值和Lead值**。

==举一个例子：==

> 接下来，我给出一张使用JConsole工具监控此JMX指标的截图。从这张图片中，我们可以看到，client-id为consumer-1的消费者在给定的测量周期内最大的Lag值为714202，最小的Lead值是83，这说明此消费者有很大的消费滞后性。
>
> <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/598a8e2c16efb23b1dc07376773c7252.png" alt="img" style="zoom: 50%;" />
>
> **Kafka消费者还在分区级别提供了额外的JMX指标，用于单独监控分区级别的Lag和Lead值**。JMX名称为：kafka.consumer:type=consumer-fetch-manager-metrics,partition=“{partition}”,topic=“{topic}”,client-id=“{client-id}”。
>
> 在我们的例子中，client-id还是consumer-1，主题和分区分别是test和0。下图展示出了分区级别的JMX指标：
>
> <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/850e91e0025c2443aebce21a29ac784a.png" alt="img" style="zoom:50%;" />
>
> 分区级别的JMX指标中多了records-lag-avg和records-lead-avg两个属性，可以计算平均的Lag值和Lead值。在实际场景中，我们会更多地使用这两个JMX指标。
>



### 小结

我今天完整地介绍了监控消费者组以及独立消费者程序消费进度的3种方法。从使用便捷性上看，应该说方法1是最简单的，我们直接运行Kafka自带的命令行工具即可。方法2使用Consumer API组合计算Lag，也是一种有效的方法，重要的是它能集成进很多企业级的自动化监控工具中。不过，集成性最好的还是方法3，直接将JMX监控指标配置到主流的监控框架就可以了。

在真实的线上环境中，我建议你优先考虑方法3，同时将方法1和方法2作为备选，装进你自己的工具箱中，随时取出来应对各种实际场景。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/c2a03833838589fa5839c7c27f3982e2.jpg" alt="img" style="zoom:25%;" />





# 4、深入Kafka内核



## 4.1 Kafka副本机制详解

所谓的副本机制（Replication），也可以称之为备份机制，通常是指分布式系统在多台网络互联的机器上保存有相同的数据拷贝。副本机制有什么好处呢？

1. **提供数据冗余**。即使系统部分组件失效，系统依然能够继续运转，因而增加了整体可用性以及数据持久性。
2. **提供高伸缩性**。支持横向扩展，能够通过增加机器的方式来提升读性能，进而提高读操作吞吐量。
3. **改善数据局部性**。允许将数据放入与用户地理位置相近的地方，从而降低系统延时。

==对于Apache Kafka而言，目前只能享受到副本机制带来的第1个好处==，也就是提供数据冗余实现高可用性和高持久性。我会在这一讲后面的内容中，详细解释Kafka没能提供第2点和第3点好处的原因。



### 4.1.1 副本定义

副本的概念实际上是在分区层级下定义的，每个分区配置有若干个副本。

**所谓副本（Replica），本质就是一个只能追加写消息的提交日志**。根据Kafka副本机制的定义，同一个分区下的所有副本保存有相同的消息序列，这些副本分散保存在不同的Broker上，从而能够对抗部分Broker宕机带来的数据不可用。

在实际生产环境中，每台Broker都可能保存有各个主题下不同分区的不同副本，因此，单个Broker上存有成百上千个副本的现象是非常正常的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/b600557f4f11dcc644813f46cbbc03d3.png" alt="img" style="zoom:67%;" />

### 4.1.2 副本工作方式

我们该如何确保副本中所有的数据都是一致的呢？特别是对Kafka而言，当生产者发送消息到某个主题后，消息是如何同步到对应的所有副本中的呢？针对这个问题，最常见的解决方案就是采用**基于领导者（Leader-based）的副本机制**。

基于Leader的副本机制的工作原理如下图所示

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/2fa6fef8d596f046b628a3befa8d6d9f.png" alt="img" style="zoom: 25%;" />

- 第一，在Kafka中，副本分成两类：领导者副本（Leader Replica）和追随者副本（Follower Replica）。每个分区在创建时都要选举一个副本，称为领导者副本，其余的副本自动称为追随者副本。

- **第二，在Kafka中，追随者follower副本是不对外提供服务的。**这就是说，任何一个追随者副本都不能响应消费者和生产者的读写请求。所有的请求都必须由领导者副本来处理，它唯一的任务就是从领导者副本异步拉取消息，并写入到自己的提交日志中，从而实现与领导者副本的同步。

- ==第三，当领导者副本挂掉了，Kafka依托于ZooKeeper提供的监控功能能够实时感知到==，并立即开启新一轮的领导者选举，从追随者副本中选一个作为新的领导者。老Leader副本重启回来后，只能作为追随者副本加入到集群中。

==你一定要特别注意上面的第二点，即追随者副本是不对外提供服务的。==还记得刚刚我们谈到副本机制的好处时，说过Kafka没能提供读操作横向扩展以及改善局部性吗？具体的原因就在于此。

> 对于客户端用户而言，Kafka的追随者副本没有任何作用，它既不能像MySQL那样帮助领导者副本“抗读”，也不能实现将某些副本放到离客户端近的地方来改善数据局部性。

==既然如此，Kafka为什么要这样设计呢？其实这种副本机制有两个方面的好处。==

- 1.**方便实现“Read-your-writes”**。

  所谓Read-your-writes，顾名思义就是，当你使用生产者API向Kafka成功写入消息后，马上使用消费者API去读取刚才生产的消息。

  举个例子，比如你平时发微博时，你发完一条微博，肯定是希望能立即看到的，这就是典型的Read-your-writes场景。如果允许追随者副本对外提供服务，由于副本同步是异步的，因此有可能出现追随者副本还没有从领导者副本那里拉取到最新的消息，从而使得客户端看不到最新写入的消息。

- 2.**方便实现单调读（Monotonic Reads）**。

  什么是单调读呢？就是对于一个消费者用户而言，在多次消费消息时，它不会看到某条消息一会儿存在一会儿不存在。

  如果允许追随者副本提供读服务，那么假设当前有2个追随者副本F1和F2，它们异步地拉取领导者副本数据。倘若F1拉取了Leader的最新消息而F2还未及时拉取，那么，此时如果有一个消费者先从F1读取消息之后又从F2拉取消息，它可能会看到这样的现象：第一次消费时看到的最新消息在第二次消费时不见了，这就不是单调读一致性。但是，如果所有的读请求都是由Leader来处理，那么Kafka就很容易实现单调读一致性。
  
  

### 4.1.3 In-sync Replicas（ISR）

我们刚刚反复说过，追随者副本不提供服务，只是定期地异步拉取领导者副本中的数据而已。既然是异步的，就存在着不可能与Leader实时同步的风险。

==什么是ISR？==

**1、Kafka引入了In-sync Replicas**，也就是所谓的ISR副本集合。ISR中的副本都是与Leader同步的副本，相反，不在ISR中的追随者副本就被认为是与Leader不同步的。

**2、要明确的是，Leader副本天然就在ISR中。**即ISR不只是追随者副本集合，它必然包括Leader副本。甚至在某些情况下，ISR只有Leader这一个副本。

**3、能够进入到ISR的追随者副本要满足一定的条件**

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/df4824e3ae53e7aebd03c38d8859aae0.png" alt="img" style="zoom: 25%;" />

> 图中有3个副本：1个领导者副本和2个追随者副本。Leader副本当前写入了10条消息，Follower1副本同步了其中的6条消息，而Follower2副本只同步了其中的3条消息。现在，请你思考一下，对于这2个追随者副本，你觉得哪个追随者副本与Leader不同步？
>
> 答案是，要根据具体情况来定。换成英文，就是那句著名的“It depends”。看上去好像Follower2的消息数比Leader少了很多，它是最有可能与Leader不同步的。的确是这样的，但仅仅是可能。

==follower副本与Leader同步的标准？==

**Broker端参数replica.lag.time.max.ms参数值：**Follower副本落后Leader副本的最长时间间隔，当前默认值是10秒。

> 这就是说，只要一个Follower副本落后Leader副本的时间不连续超过10秒，那么Kafka就认为该Follower副本与Leader是同步的，即使此时Follower副本中保存的消息明显少于Leader副本中的消息。

- 不同步时就会把该副本提出ISR集合。

- 除此之外，如果该副本后面追上了Leader的进度，那么它是能够重新被加回ISR的。

所以ISR是一个动态调整的集合，而非静态不变的。



### 4.1.4 Unclean领导者选举

（Unclean Leader Election）

既然ISR是可以动态调整的，那么自然就可以出现这样的情形：ISR为空。因为Leader副本天然就在ISR中，如果ISR为空了，就说明Leader副本也“挂掉”了，Kafka需要重新选举一个新的Leader。

==如果ISR是空，此时该怎么选举新Leader呢？==

**Kafka把所有不在ISR中的存活副本都称为非同步副本**。通常来说，非同步副本落后Leader太多，如果选择这些副本作为新Leader，就可能出现数据的丢失，选举这种副本的过程称为Unclean领导者选举。

**Broker端参数unclean.leader.election.enable控制是否允许Unclean领导者选举**。

- 优点：分区Leader副本一直存在，不至于停止对外提供服务，因此提升了高可用性；
- 缺点：开启Unclean领导者选举可能会造成数据丢失，牺牲了数据的一致性；



==如果你听说过CAP理论的话，==你一定知道，一个分布式系统通常只能同时满足一致性（Consistency）、可用性（Availability）、分区容错性（Partition tolerance）中的两个。显然，在这个问题上，Kafka赋予你选择C或A的权利。

你可以根据你的实际业务场景决定是否开启Unclean领导者选举。不过，我强烈建议你不要开启它，毕竟我们还可以通过其他的方式来提升高可用性。

### 小结

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/d75c01661ca5367cfd23ad92cc10e372.jpg" alt="img" style="zoom:25%;" />



## 4.2 请求是怎么被处理的？

无论是Kafka客户端还是Broker端，它们之间的交互都是通过“请求/响应”的方式完成的。比如，客户端会通过网络发送消息生产请求给Broker，而Broker处理完成后，会发送对应的响应给到客户端。

==下面详细讨论一下Broker端处理请求的全流程。==

Apache Kafka自己定义了一组请求协议，用于实现各种各样的交互操作。比如常见的PRODUCE请求是用于生产消息的，FETCH请求是用于消费消息的，METADATA请求是用于请求Kafka集群元数据信息的。

**所有的请求都是通过TCP网络以Socket的方式进行通讯的。**

> 关于如何处理请求，我们很容易想到的方案有两个。
>
> 1.顺序处理请求：
>
> ```
> while (true) {
>             Request request = accept(connection);
>             handle(request);
> }
> ```
>
> 这个方法实现简单，但是有个致命的缺陷，那就是吞吐量太差。
>
> **2.多线程处理**。也就是说，我们为每个入站请求都创建一个新的线程来异步处理。
>
> ```
> while (true) {
>             Request = request = accept(connection);
>             Thread thread = new Thread(() -> {
> 	handle(request);});
>             thread.start();
> }
> ```
>
> 这个方法反其道而行之，完全采用异步的方式。系统会为每个入站请求都创建单独的线程来处理。这个方法的好处是，它是完全异步的，每个请求的处理都不会阻塞下一个请求。但缺陷也同样明显。为每个请求都创建线程的做法开销极大，在某些场景下甚至会压垮整个服务。
>
> 既然这两种方案都不好，那么，Kafka是如何处理请求的呢？



==用一句话概括就是，Kafka使用的是**Reactor模式**。==

> ​	谈到Reactor模式，大神Doug Lea的“[Scalable IO in Java](http://gee.cs.oswego.edu/dl/cpjslides/nio.pdf)”应该算是最好的入门教材了。即使你没听说过Doug Lea，那你应该也用过ConcurrentHashMap吧？这个类就是这位大神写的。其实，整个java.util.concurrent包都是他的杰作！

简单来说，**Reactor模式是事件驱动架构的一种实现方式，特别适合应用于处理多个客户端并发向服务器端发送请求的场景**。我借用Doug Lea的一页PPT来说明一下Reactor的架构，并借此引出Kafka的请求处理模型。

Reactor模式的架构如下图所示：

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/654b83dc6b24d89c138938c15d2e8352.png" alt="img" style="zoom: 33%;" />

从这张图中，我们可以发现，多个客户端会发送请求给到Reactor。==Reactor有个请求分发线程Dispatcher==，图中的Acceptor，它会将不同的请求下发到多个工作线程中处理。

**在这个架构中，Acceptor线程只是用于请求分发，**不涉及具体的逻辑处理，非常得轻量级，因此有很高的吞吐量表现。而这些工作线程可以根据实际业务处理需要任意增减，从而动态调节系统负载能力。

==如果我们来为Kafka画一张类似的图的话，那它应该是这个样子的：==

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/e1ae8884999175dac0c6e21beb2f7e6e.png" alt="img" style="zoom: 33%;" />

**Kafka的Broker端有个SocketServer组件，**类似于Reactor模式中的Dispatcher，它也有对应的Acceptor线程和一个工作线程池，只不过在Kafka中，这个工作线程池有个专属的名字，叫网络线程池。

> Kafka提供了Broker端参数num.network.threads，用于调整该网络线程池的线程数。其默认值是3，表示每台Broker启动时会创建3个网络线程，专门处理客户端发送的请求。

Acceptor线程采用轮询的方式将入站请求公平地发到所有网络线程中

目前，我们已经知道客户端发来的请求会被Broker端的Acceptor线程分发到任意一个网络线程中，由它们来进行处理。

==那么，当网络线程池子接收到请求后，它是怎么处理的呢？==

你可能会认为，它顺序处理不就好了吗？实际上，Kafka在这个环节又做了一层异步线程池的处理，我们一起来看一看下面这张图。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/d8a7d6f0bdf9dc3af4ff55ff79b42068.png" alt="img" style="zoom:33%;" />

**1、当网络线程拿到请求后，它不是自己处理，而是将请求放入到一个共享请求队列中。**

**2、Broker端还有个IO线程池，负责从该队列中取出请求，执行真正的处理。**如果是PRODUCE生产请求，则将消息写入到底层的磁盘日志中；如果是FETCH请求，则从磁盘或页缓存中读取消息。IO线程池处中的线程才是执行请求逻辑的线程。

> Broker端参数num.io.threads控制了这个线程池中的线程数。目前该参数默认值是8，表示每台Broker启动后自动创建8个IO线程处理请求。你可以根据实际硬件条件设置此线程池的个数。

**3、当IO线程处理完请求后，会将生成的响应发送到网络线程池的响应队列中，**然后由对应的网络线程（谁发到我们IO里面的谁负责）负责将Response直接返还给客户端。

细心的你一定发现了请求队列和响应队列的差别：**请求队列是所有网络线程共享的，而响应队列则是每个网络线程专属的**。这么设计的原因就在于，Dispatcher只是用于请求分发而不负责响应回传，因此只能让每个网络线程自己发送Response给客户端，所以这些Response也就没必要放在一个公共的地方。

==Purgatory的组件是什么？==

**图中有一个叫Purgatory的组件，这是Kafka中著名的“炼狱”组件。它是用来缓存延时请求（Delayed Request）的。**

所谓延时请求，就是那些不能立刻处理的请求。比如设置了acks=all的PRODUCE请求，一旦设置了acks=all，那么该请求就必须等待ISR中所有副本都接收了消息后才能返回，此时处理该请求的IO线程就必须等待其他Broker的写入结果。当请求不能立刻处理时，它就会暂存在Purgatory中。稍后一旦满足了完成条件，IO线程会继续处理该请求，并将Response放入对应网络线程的响应队列中。





## 4.3 消费者组重平衡全流程解析

消费者组的重平衡流程，作用是让组内所有的消费者实例就消费哪些主题分区达成一致。重平衡需要借助Kafka Broker端的Coordinator组件，在Coordinator的帮助下完成整个消费者组的分区重分配。今天我们就来详细说说这个流程。

> 先提示一下，我会以Kafka 2.3版本的源代码开启今天的讲述。在分享的过程中，对于旧版本的设计差异，我也会显式地说明。这样，即使你依然在使用比较旧的版本也不打紧，毕竟设计原理大体上是没有变化的。

### 4.3.1 触发与通知

我们先来简单回顾一下重平衡的3个触发条件：

1. 组成员数量发生变化。
2. 订阅主题数量发生变化。
3. 订阅主题的分区数发生变化。

在实际生产环境中，因命中第1个条件而引发的重平衡是最常见的。另外，消费者组中的消费者实例依次启动也属于第1种情况，也就是说，每次消费者组启动时，必然会触发重平衡过程。

今天，我真正想引出的是另一个话题：

==Broker端Coordinator组件，如何通知rebalance到其他消费者实例的？答案就是，靠消费者端的心跳线程（Heartbeat Thread）==

Kafka Java消费者需要定期地发送心跳请求（Heartbeat Request）到Broker端的Coordinator，以表明它还存活着。

**重平衡的通知机制正是通过心跳线程来完成的**。当协调者决定开启新一轮重平衡后，它会将“REBALANCE_IN_PROGRESS”封装进心跳请求的响应中，发还给消费者实例。当消费者实例发现心跳响应中包含了“REBALANCE_IN_PROGRESS”，就能立马知道重平衡又开始了，这就是重平衡的通知机制。

对了，很多人还搞不清楚消费者端参数heartbeat.interval.ms的真实用途，我来解释一下。从字面上看，它就是设置了心跳的间隔时间，但这个参数的真正作用是控制重平衡通知的频率。如果你想要消费者实例更迅速地得到通知，那么就可以给这个参数设置一个非常小的值，这样消费者就能更快地感知到重平衡已经开启了。



### 4.3.2 消费组状态机

重平衡一旦开启，Broker端的协调者组件就要开始忙了，主要涉及到控制消费者组的状态流转。

Kafka为消费者组定义了5种状态，它们分别是：Empty、Dead、PreparingRebalance、CompletingRebalance和Stable。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/3c281189cfb1d87173bc2d4b8149f38b.jpeg" alt="img" style="zoom: 67%;" />

了解了这些状态的含义之后，我们来看一张图片，它展示了状态机的各个状态流转。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/f16fbcb798a53c21c3bf1bcd5b72b006.png" alt="img" style="zoom: 67%;" />

我来解释一下消费者组启动时的状态流转过程。一个消费者组最开始是Empty状态，当重平衡过程开启后，它会被置于PreparingRebalance状态等待成员加入，之后变更到CompletingRebalance状态等待分配方案，最后流转到Stable状态完成重平衡。

当有新成员加入或已有成员退出时，消费者组的状态从Stable直接跳到PreparingRebalance状态，此时，所有现存成员就必须重新申请加入组。当所有成员都退出组后，消费者组状态变更为Empty。Kafka定期自动删除过期位移的条件就是，组要处于Empty状态。因此，如果你的消费者组停掉了很长时间（超过7天），那么Kafka很可能就把该组的位移数据删除了。

我相信，你在Kafka的日志中一定经常看到下面这个输出：

> *Removed ✘✘✘ expired offsets in ✘✘✘ milliseconds.*

这就是Kafka在尝试定期删除过期位移。现在你知道了，只有Empty状态下的组，才会执行过期位移删除的操作。



### 4.3.3 消费者重平衡流程

> 有了上面的内容作铺垫，我们就可以开始介绍重平衡流程了。

**重平衡的完整流程需要消费者和协调者组件共同参与才能完成。**

==在消费者端，重平衡分为两个步骤：==分别是加入组和等待分配方案。

这两个步骤分别对应两类特定的请求：JoinGroup请求和SyncGroup请求。

- **1、JoinGroup请求**

  当组内成员加入组时，它会向协调者发送JoinGroup请求。在该请求中，每个成员都要将自己订阅的主题上报，这样协调者就能收集到所有成员的订阅信息

  一旦收集了全部成员的JoinGroup请求后，协调者会从这些成员中选择一个担任这个消费者组的领导者。

  > 通常情况下，第一个发送JoinGroup请求的成员自动成为领导者。你一定要注意区分这里的领导者和之前我们介绍的领导者副本，它们不是一个概念。这里的领导者是具体的消费者实例，它既不是副本，也不是协调者。
  >
  > **领导者消费者的任务是收集所有成员的订阅信息，然后根据这些信息，制定具体的分区消费分配方案。**

  选出领导者之后，协调者会把消费者组订阅信息封装进JoinGroup请求的响应体中，然后发给领导者，由领导者统一做出分配方案后，进入到下一步：发送SyncGroup请求。

- **2、SyncGroup请求：**

  在这一步中，领导者向协调者发送SyncGroup请求，将刚刚做出的分配方案发给协调者。值得注意的是，其他成员也会向协调者发送SyncGroup请求，只不过请求体中并没有实际的内容。

  这一步的主要目的是让协调者接收分配方案，然后统一以SyncGroup响应的方式分发给所有成员，这样组内所有成员就都知道自己该消费哪些分区了。

接下来，我用一张图来形象地说明一下JoinGroup请求的处理过程。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/e7d40ce1c34d66ec36bfdaaa3ec9611f.png" alt="img" style="zoom: 33%;" />

下面这张图描述的是SyncGroup请求的处理流程。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/6252b051450c32c143f03410f6c2b75d.png" alt="img" style="zoom:25%;" />

SyncGroup请求的主要目的，就是让协调者把领导者制定的分配方案下发给各个组内成员。当所有成员都成功接收到分配方案后，消费者组进入到Stable状态，即开始正常的消费工作。



### 4.3.4 Broker端重平衡场景剖析

==要剖析Broker端Coordinator 处理重平衡的全流程，我们必须要分几个场景来讨论。==这几个场景分别是新成员加入组、组成员主动离组、组成员崩溃离组、组成员提交位移。

**场景一：新成员入组。**

新成员入组是指组处于Stable状态后，有新成员加入。如果是全新启动一个消费者组，Kafka是有一些自己的小优化的，流程上会有些许的不同。

当协调者收到新的JoinGroup请求后，它会通过心跳请求响应的方式通知组内现有的所有成员，强制它们开启新一轮的重平衡。具体的过程和之前的客户端重平衡流程是一样的。现在，我用一张时序图来说明协调者一端是如何处理新成员入组的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/62f85fb0b0f06989dd5a6f133599ca33.png" alt="img" style="zoom: 50%;" />

**场景二：组成员主动离组。**

何谓主动离组？就是指消费者实例所在线程或进程调用close()方法主动通知协调者它要退出。这个场景就涉及到了第三类请求：**LeaveGroup请求**。协调者收到LeaveGroup请求后，依然会以心跳响应的方式通知其他成员，因此我就不再赘述了，还是直接用一张图来说明。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/867245cbf6cfd26573aba1816516b26b.png" alt="img" style="zoom:50%;" />

**场景三：组成员崩溃离组。**

**崩溃离组是指消费者实例出现严重故障，突然宕机导致的离组**。它和主动离组是有区别的，因为后者是主动发起的离组，协调者能马上感知并处理。但崩溃离组是被动的，协调者通常需要等待一段时间才能感知到，这段时间一般是由消费者端参数session.timeout.ms控制的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/bc00d35060e1a4216e177e5b361ad40c.png" alt="img" style="zoom:50%;" />

**场景四：重平衡时，协调者对组内成员提交位移的处理。**

正常情况下，每个组内成员都会定期汇报位移给协调者。当重平衡开启时，协调者会给予成员一段缓冲时间，要求每个成员必须在这段时间内快速地上报自己的位移信息，然后再开启正常的JoinGroup/SyncGroup请求发送。还是老办法，我们使用一张图来说明。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/83b77094d4170b9057cedfed9cdb33be.png" alt="img" style="zoom:50%;" />

### 小结



<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/6f0aaf535180899b16923dc3c76ad373.jpg" alt="img" style="zoom:25%;" />



## 4.4 Kafka控制器controller



**控制器组件（Controller），是Kafka的核心组件。它的主要作用是在 ZooKeeper的帮助下管理和协调整个Kafka集群**。

集群中任意一台Broker都能充当控制器的角色，但是，在运行过程中，只能有一个Broker成为控制器，行使其管理和协调的职责。

==下面，我们就来详细说说控制器的原理和内部运行机制。==

> 在开始之前，我先简单介绍一下Apache ZooKeeper框架。要知道，**控制器是重度依赖ZooKeeper的**。
>
> **Apache ZooKeeper是一个提供高可靠性的分布式协调服务框架**。它使用的数据模型类似于文件系统的树形结构，根目录也是以“/”开始。该结构上的每个节点被称为znode，用来保存一些元数据协调信息。
>
> 如果以znode持久性来划分，**znode可分为持久性znode和临时znode**。持久性znode不会因为ZooKeeper集群重启而消失，而临时znode则与创建该znode的ZooKeeper会话绑定，一旦会话结束，该节点会被自动删除。
>
> **ZooKeeper赋予客户端监控znode变更的能力，即所谓的Watch通知功能。**一旦znode节点被创建、删除，子节点数量发生变化，抑或是znode所存的数据本身变更，ZooKeeper会通过节点变更监听器(ChangeHandler)的方式显式通知客户端。
>
> 依托于这些功能，ZooKeeper常被用来实现**集群成员管理、分布式锁、领导者选举**等功能。Kafka控制器大量使用Watch功能实现对集群的协调管理。我们一起来看一张图片，它展示的是Kafka在ZooKeeper中创建的znode分布。你不用了解每个znode的作用，但你可以大致体会下Kafka对ZooKeeper的依赖。
>
> <img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/81caaddbf27cb7f938fb41dc3737c1f3.jpg" alt="img" style="zoom:25%;" />
>
> 掌握了ZooKeeper的这些基本知识，现在我们就可以开启对Kafka控制器的讨论了。



### 4.4.1 控制器是如何被选出来的？

你一定很想知道，控制器是如何被选出来的呢？我们刚刚在前面说过，每台Broker都能充当控制器，那么，当集群启动后，Kafka怎么确认控制器位于哪台Broker呢？

实际上，Broker在启动时，会尝试去ZooKeeper中创建/controller节点。

**Kafka当前选举控制器的规则是：第一个成功创建/controller节点的Broker会被指定为控制器。**



### 4.4.2 控制器是做什么的？

我控制器是起协调作用的组件，那么，这里的协调作用到底是指什么呢？控制器的职责大致可以分为5种。

- **1.主题管理（创建、删除、增加分区）**

这里的主题管理，就是指控制器帮助我们完成对Kafka主题的创建、删除以及分区增加的操作。

- **2.分区重分配**

分区重分配主要是指，kafka-reassign-partitions脚本提供的对已有主题分区进行细粒度的分配功能。这部分功能也是控制器实现的。

- **3.Preferred领导者选举**

Preferred领导者选举主要是Kafka为了避免部分Broker负载过重而提供的一种换Leader的方案。在专栏后面说到工具的时候，我们再详谈Preferred领导者选举，这里你只需要了解这也是控制器的职责范围就可以了。

- **4.集群成员管理（新增Broker、Broker主动关闭、Broker宕机）**

自动检测新增Broker、Broker主动关闭及被动宕机。这种自动检测是依赖于前面提到的Watch功能和ZooKeeper临时节点组合实现的。

> 比如，控制器组件会利用Watch机制检查ZooKeeper的/brokers/ids节点下的子节点数量变更。目前，当有新Broker启动后，它会在/brokers下创建专属的znode节点。一旦创建完毕，ZooKeeper会通过Watch机制将消息通知推送给控制器，这样，控制器就能自动地感知到这个变化，进而开启后续的新增Broker作业。
>
> 侦测Broker存活性则是依赖于刚刚提到的另一个机制：临时节点。每个Broker启动后，会在/brokers/ids下创建一个临时znode。当Broker宕机或主动关闭后，该Broker与ZooKeeper的会话结束，这个znode会被自动删除。同理，ZooKeeper的Watch机制将这一变更推送给控制器，这样控制器就能知道有Broker关闭或宕机了，从而进行“善后”。
>

- **5.数据服务**

控制器的最后一大类工作，就是向其他Broker提供数据服务。控制器上保存了最全的集群元数据信息，其他所有Broker会定期接收控制器发来的元数据更新请求，从而更新其内存中的缓存数据。



### 4.4.3 控制器保存了什么数据？

接下来，我们就详细看看，控制器中到底保存了哪些数据。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/38ff78fdeb2a86943ae60f15c3ad28c8.jpg" alt="img" style="zoom: 25%;" />

怎么样，图中展示的数据量是不是很多？几乎把我们能想到的所有Kafka集群的数据都囊括进来了。这里面比较重要的数据有：

- 所有主题信息。包括具体的分区信息，比如领导者副本是谁，ISR集合中有哪些副本等。
- 所有Broker信息。包括当前都有哪些运行中的Broker，哪些正在关闭中的Broker等。
- 所有涉及运维任务的分区。包括当前正在进行Preferred领导者选举以及分区重分配的分区列表。

值得注意的是，这些数据其实在ZooKeeper中也保存了一份。每当控制器初始化时，它都会从ZooKeeper上读取对应的元数据并填充到自己的缓存中。有了这些数据，控制器就能对外提供数据服务了。这里的对外主要是指对其他Broker而言，控制器通过向这些Broker发送请求的方式将这些数据同步到其他Broker上。



### 4.4.4 controller故障转移

在Kafka集群运行过程中，只能有一台Broker充当控制器的角色，那么这就存在**单点失效**（Single Point of Failure）的风险，Kafka是如何应对单点失效的呢？

**故障转移指的是，当运行中的控制器突然宕机或意外终止时，Kafka能够快速地感知到，并立即启用备用控制器来代替之前失败的控制器**。这个过程就被称为Failover，该过程是自动完成的，无需你手动干预。

接下来，我们一起来看一张图，它简单地展示了控制器故障转移的过程。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/128903a88ea1c9dd27f6a62e496b44ed.jpg" alt="img" style="zoom: 25%;" />

最开始时，Broker 0是控制器。当Broker 0宕机后，ZooKeeper通过Watch机制感知到并删除了/controller临时节点。之后，所有存活的Broker开始竞选新的控制器身份。Broker 3最终赢得了选举，成功地在ZooKeeper上重建了/controller节点。之后，Broker 3会从ZooKeeper中读取集群元数据信息，并初始化到自己的缓存中。至此，控制器的Failover完成，可以行使正常的工作职责了。



### 小结

好了，有关Kafka控制器的内容，我已经讲完了。最后，我再跟你分享一个小窍门。当你觉得控制器组件出现问题时，比如主题无法删除了，或者重分区hang住了，你不用重启Kafka Broker或控制器。有一个简单快速的方式是，去ZooKeeper中手动删除/controller节点。**具体命令是rmr /controller**。这样做的好处是，既可以引发控制器的重选举，又可以避免重启Broker导致的消息处理中断。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/a77479402c0fddbf7541d26d72a97707.jpg" alt="img" style="zoom:25%;" />

==目前，控制器依然是重度依赖于ZooKeeper的。未来如果要减少对ZooKeeper的依赖，你觉得可能的方向是什么？==



## 4.5 高水位和Leader Epoch

你可能听说过高水位（High Watermark），但不一定耳闻过Leader Epoch。前者是Kafka中非常重要的概念，而后者是社区在0.11版本中新推出的，主要是为了弥补高水位机制的一些缺陷。**鉴于高水位机制在Kafka中举足轻重，而且深受各路面试官的喜爱**，今天我们就来重点说说高水位。



### 4.5.1 什么是高水位？

首先，我们要明确一下基本的定义：什么是高水位？或者说什么是水位？水位一词多用于流式处理领域，比如，Spark Streaming或Flink框架中都有水位的概念。教科书中关于水位的经典定义通常是这样的：

> 在时刻T，任意创建时间（Event Time）为T’，且T’≤T的所有事件都已经到达或被观测到，那么T就被定义为水位。

“Streaming System”一书则是这样表述水位的：

> 水位是一个单调增加且表征最早未完成工作（oldest work not yet completed）的时间戳。

为了帮助你更好地理解水位，我借助这本书里的一张图来说明一下。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/fb2c9e883b78c5d10b09b4a9773b8c13.png" alt="img" style="zoom: 33%;" />

图中标注“Completed”的蓝色部分代表已完成的工作，标注“In-Flight”的红色部分代表正在进行中的工作，两者的边界就是水位线。

在Kafka的世界中，水位的概念有一点不同。Kafka的水位不是时间戳，更与时间无关。它是和位置信息绑定的，具体来说，它是用消息位移来表征的。另外，Kafka源码使用的表述是高水位，因此，今天我也会统一使用“高水位”或它的缩写HW来进行讨论。

### 4.5.2 高水位的作用

==在Kafka中，高水位的作用主要有2个:==

1. 定义消息对外的可见性，即用来标识分区下的哪些消息是可以被消费的。
2. 帮助Kafka完成副本同步。

下面这张图展示了多个与高水位相关的Kafka术语。 

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/c2243d5887f0ca7a20a524914b85a8dd.png" alt="img" style="zoom:33%;" />

我们假设这是某个分区Leader副本的高水位图。首先，请你注意图中的“已提交消息”和“未提交消息”。我们之前在谈到Kafka持久性保障的时候，特意对两者进行了区分。现在，我借用高水位再次强调一下。

在分区高水位以下的消息被认为是已提交消息，反之就是未提交消息。消费者只能消费已提交消息，即图中位移小于8的所有消息。

**图中还有一个日志末端位移的概念，即Log End Offset，简写是LEO。**它表示副本写入下一条消息的位移值。注意，数字15所在的方框是虚线，这就说明，这个副本当前只有15条消息，位移值是从0到14，下一条新消息的位移是15。

显然，介于高水位和LEO之间的消息就属于未提交消息。同一个副本对象，其高水位值不会大于LEO值。

**高水位和LEO是副本对象的两个重要属性**。

==Kafka所有副本都有对应的高水位和LEO值而不仅仅是Leader副本==。只不过Leader副本比较特殊，Kafka使用Leader副本的高水位来定义所在分区的高水位。**分区的高水位就是其Leader副本的高水位**。



### 4.5.3 高水位更新机制

现在，我们知道了每个副本对象都保存了一组高水位值和LEO值，但实际上，==在Leader副本所在的Broker上，还保存了其他Follower副本的LEO值==。我们一起来看看下面这张图。

在这张图中，我们可以看到，Broker 0上保存了某分区的Leader副本和所有Follower副本的LEO值，而Broker 1上仅仅保存了该分区的某个Follower副本。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/be0c738f34e3cd1d95d509f16cbb7f82.png" alt="img" style="zoom: 33%;" />

==Kafka把Leader副本Broker保存的Follower副本称为远程副本（Remote Replica）==

- Kafka副本机制在运行过程中，不会更新远程副本的高水位值（图中灰色的部分）。 

  其实对于整个同步过程而言，这个follower副本的HW一点用没有，它存在的原因在于Leader副本宕机后，你懂吧，Leader副本只要知道所有follower副本的LEO值就可以了，用里面的最小值来定位当前的HW的高低。

==为什么要在Broker 0上保存这些远程副本呢？== 

其实它主要作用是，**帮助Leader副本确定其高水位，也就是分区高水位**。

为了帮助你更好地记忆这些值被更新的时机，我做了一张表格。只有搞清楚了更新机制，我们才能开始讨论Kafka副本机制的原理，以及它是如何使用高水位来执行副本消息同步的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/c81e888761b5f04822216845be981649.jpeg" alt="img" style="zoom: 67%;" />

==什么叫与Leader副本保持同步==。

判断的条件有两个，满足其一就可以：

1. 该远程Follower副本在ISR中。
2. 该远程Follower副本LEO值落后于Leader副本LEO值的时间，不超过Broker端参数replica.lag.time.max.ms的值。如果使用默认值的话，就是不超过10秒。

乍一看，这两个条件好像是一回事，因为目前某个副本能否进入ISR就是靠第2个条件判断的。但有些时候，会发生这样的情况：即Follower副本已经“追上”了Leader的进度，却不在ISR中，比如某个刚刚重启回来的副本。如果Kafka只判断第1个条件的话，就可能出现某些副本具备了“进入ISR”的资格，但却尚未进入到ISR中的情况。此时，分区高水位值就可能超过ISR中副本LEO，而高水位 > LEO的情形是不被允许的。

==下面，我们分别从Leader副本和Follower副本两个维度，来总结一下高水位和LEO的更新机制。==

==Leader副本==

处理生产者请求的逻辑如下：

1. 写入消息到本地磁盘。
2. 更新分区高水位值。
   1. 获取所在Broker端的所有远程副本LEO值（LEO-1，LEO-2，……，LEO-n）
   2. 获取Leader副本高水位值：currentHW。
   3. 更新 currentHW = **max** { currentHW,  **min**( LEO-1, LEO-2, ……，LEO-n) }。


处理Follower副本拉取消息的逻辑如下：

1. 读取磁盘（或页缓存）中的消息数据
2. 使用Follower副本发送请求中的offset值更新远程副本LEO值（这个offset是从哪里开始拉取的位移值，不是拉取数据量后的位移值）
3. 更新分区高水位值（同上）



==Follower副本==

从Leader拉取消息的处理逻辑如下：

1. 写入消息到本地磁盘
2. 更新LEO值
3. 更新高水位值==（？？？？）==
   1. 获取Leader发送的高水位值：currentHW。
   2. 更新高水位为 min (currentHW, currentLEO)。




### 4.5.4 副本同步机制解析例子

搞清楚了这些值的更新机制之后，我来举一个实际的例子，说明一下Kafka副本同步的全流程。

该例子使用一个单分区且有两个副本的主题。当生产者发送一条消息时，Leader和Follower副本对应的高水位是怎么被更新的呢？我给出了一些图片，我们一一来看。

**1、首先是初始状态。**下面这张图中的remote LEO就是远程副本的LEO值。在初始状态时，所有值都是0。

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/2ecec2915d1a52f136517d15192a4c72.png)

**2、当生产者给主题分区发送一条消息后，状态变更为：**

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/42841bfd3d5d4fa8560e176cb9d20b5b.png)

此时，Leader副本成功将消息写入了本地磁盘，故LEO值被更新为1。

Follower再次尝试从Leader拉取消息，这次有消息可以拉取了，因此状态进一步变更为：

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/f65911a5c247ad83826788fd275e1ade.png)

这时，Follower副本也成功地更新LEO为1，所以Leader和Follower副本的LEO都是1；

==但各自的高水位依然是0，还没有被更新，它们只能在下一轮的拉取中被更新，如下图所示：==

==并且，Leader副本的高水位会先更新，然后在Leader给follwer发送返回值后，follower才会更新，两者时间存在不同点==

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/f30a4651605352db542b76b3512df110.png)

- **在下一次的拉取请求中，Follower副本这次请求拉取的是位移值=1的消息，此时Leader就知道follow前面的消息同步成功了。**

  先更新远程副本LEO为1，再更新Leader高水位为1。

- **然后 在给follower的返回值中，会将高水位值1发送给Follower副本。**Follower副本接收到以后，也将自己的高水位值更新成1。

至此，一次完整的消息同步周期就结束了。



### 4.5.5 Leader Epoch登场

故事讲到这里似乎很完美，依托于高水位，Kafka既界定了消息的对外可见性，又实现了异步的副本同步机制。不过，我们还是要思考一下这里面存在的问题。

==从刚才的分析中，我们知道，Follower副本的高水位更新需要一轮额外的拉取请求才能实现！==

如果把上面那个例子扩展到多个Follower副本，情况可能更糟，也许需要多轮拉取请求。也就是说，Leader副本高水位更新和Follower副本高水位更新在时间上是存在错配的。这种错配是很多“数据丢失”或“数据不一致”问题的根源。基于此，社区在0.11版本正式引入了Leader Epoch概念，来规避因高水位更新错配导致的各种不一致问题。

所谓Leader Epoch，我们大致可以认为是Leader版本。它由两部分数据组成。

1. Epoch。一个单调增加的版本号。每当副本领导权发生变更时，都会增加该版本号。小版本号的Leader被认为是过期Leader，不能再行使Leader权力。
2. 起始位移（Start Offset）。Leader副本在该Epoch值上写入的首条消息的位移。

> 我举个例子来说明一下Leader Epoch。 
>
> 假设现在有两个Leader Epoch<0, 0>和<1, 120>，那么，第一个Leader Epoch表示版本号是0，这个版本的Leader从位移0开始保存消息，一共保存了120条消息。之后，Leader发生了变更，版本号增加到1，新版本的起始位移是120。

Kafka Broker会在内存中为每个分区都缓存Leader Epoch数据，同时它还会定期地将这些信息持久化到一个checkpoint文件中。当Leader副本写入消息到磁盘时，Broker会尝试更新这部分缓存。如果该Leader是首次写入消息，那么Broker会向缓存中增加一个Leader Epoch条目，否则就不做更新。这样，每次有Leader变更时，新的Leader副本会查询这部分缓存，取出对应的Leader Epoch的起始位移，以避免数据丢失和不一致的情况。

接下来，我们来看一个实际的例子，它展示的是Leader Epoch是如何防止数据丢失的。请先看下图。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/69f8ccf346b568a7310c69de9863ca42.png" alt="img" style="zoom: 50%;" />

==我稍微解释一下，单纯依赖高水位是怎么造成数据丢失的。==

开始时，副本A和副本B都处于正常状态，A是Leader副本。某个使用了默认acks设置的生产者程序向A发送了两条消息，A全部写入成功，此时Kafka会通知生产者说两条消息全部发送成功。

现在我们假设Leader和Follower都写入了这两条消息，==而且Leader副本的高水位也已经更新了，但Follower副本高水位还未更新==——这是可能出现的。还记得吧，Follower端高水位的更新与Leader端有时间错配。倘若此时副本B所在的Broker宕机，当它重启回来后，副本B会执行日志截断操作，将LEO值调整为之前的高水位值，也就是1。这就是说，位移值为1的那条消息被副本B从磁盘中删除，此时副本B的底层磁盘文件中只保存有1条消息，即位移值为0的那条消息。

当执行完截断操作后，副本B开始从A拉取消息，执行正常的消息同步。如果就在这个节骨眼上，副本A所在的Broker宕机了，那么Kafka就别无选择，只能让副本B成为新的Leader，此时，当A回来后，需要执行相同的日志截断操作，即将高水位调整为与B相同的值，也就是1。这样操作之后，位移值为1的那条消息就从这两个副本中被永远地抹掉了。这就是这张图要展示的数据丢失场景。

==严格来说，这个场景发生的前提是**Broker端参数min.insync.replicas设置为1**。==此时一旦消息被写入到Leader副本的磁盘，就会被认为是“已提交状态”，但现有的时间错配问题导致Follower端的高水位更新是有滞后的。如果在这个短暂的滞后时间窗口内，接连发生Broker宕机，那么这类数据的丢失就是不可避免的。

现在，我们来看下如何利用Leader Epoch机制来规避这种数据丢失。我依然用图的方式来说明。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/1078956136267ca958d82bfa16d825e1.png" alt="img" style="zoom:50%;" />

场景和之前大致是类似的，只不过引用Leader Epoch机制后，Follower副本B重启回来后，需要向A发送一个特殊的请求去获取Leader的LEO值。在这个例子中，该值为2。当获知到Leader LEO=2后，B发现该LEO值不比它自己的LEO值小，而且缓存中也没有保存任何起始位移值 > 2的Epoch条目，因此B无需执行任何日志截断操作。这是对高水位机制的一个明显改进，即副本是否执行日志截断不再依赖于高水位进行判断。

现在，副本A宕机了，B成为Leader。同样地，当A重启回来后，执行与B相同的逻辑判断，发现也不用执行日志截断，至此位移值为1的那条消息在两个副本中均得到保留。后面当生产者程序向B写入新消息时，副本B所在的Broker缓存中，会生成新的Leader Epoch条目：[Epoch=1, Offset=2]。之后，副本B会使用这个条目帮助判断后续是否执行日志截断操作。这样，通过Leader Epoch机制，Kafka完美地规避了这种数据丢失场景。

### 小结

今天，我向你详细地介绍了Kafka的高水位机制以及Leader Epoch机制。高水位在界定Kafka消息对外可见性以及实现副本机制等方面起到了非常重要的作用，但其设计上的缺陷给Kafka留下了很多数据丢失或数据不一致的潜在风险。为此，社区引入了Leader Epoch机制，尝试规避掉这类风险。事实证明，它的效果不错，在0.11版本之后，关于副本数据不一致性方面的Bug的确减少了很多。如果你想深入学习Kafka的内部原理，今天的这些内容是非常值得你好好琢磨并熟练掌握的。

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/989d13e4bc4f44618a10b5b7bd6f523f.jpg" alt="img" style="zoom:25%;" />





# 5、管理与监控(NO)

## 5.1 主题管理知多少?

今天我想和你讨论一下Kafka中的主题管理，包括日常的主题管理、特殊主题的管理与运维以及常见的主题错误处理。

### 5.1.1 主题日常管理

所谓的日常管理，无非就是主题的增删改查。你可能会觉得，这有什么好讨论的，官网上不都有命令吗？这部分内容的确比较简单，但它是我们讨论后面内容的基础。

另外，我们今天讨论的管理手段都是借助于Kafka自带的命令。事实上，在专栏后面，我们还会专门讨论如何使用Java API的方式来运维Kafka集群。

==1、如何使用命令创建Kafka主题==

**Kafka提供了自带的kafka-topics脚本，用于帮助用户创建主题**。一个典型的创建命令如下：

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --create --topic my_topic_name  --partitions 1 --replication-factor 1
```

create表明我们要创建主题，而partitions和replication factor分别设置了主题的分区数以及每个分区下的副本数。

> ​	如果你之前使用过这个命令，你可能会感到奇怪：难道不是指定 --zookeeper参数吗？为什么现在变成 --bootstrap-server了呢？我来给出答案：从Kafka 2.2版本开始，社区推荐用 --bootstrap-server参数替换 --zookeeper参数，并且显式地将后者标记为“已过期”，因此，如果你已经在使用2.2版本了，那么创建主题请指定 --bootstrap-server参数。
>
> 社区推荐使用 --bootstrap-server而非 --zookeeper的原因主要有两个。
>
> 1. 使用 --zookeeper会绕过Kafka的安全体系。这就是说，即使你为Kafka集群设置了安全认证，限制了主题的创建，如果你使用 --zookeeper的命令，依然能成功创建任意主题，不受认证体系的约束。这显然是Kafka集群的运维人员不希望看到的。
> 2. 使用 --bootstrap-server与集群进行交互，越来越成为使用Kafka的标准姿势。换句话说，以后会有越来越少的命令和API需要与ZooKeeper进行连接。这样，我们只需要一套连接信息，就能与Kafka进行全方位的交互，不用像以前一样，必须同时维护ZooKeeper和Broker的连接信息。

==2、创建好主题之后，Kafka允许我们使用相同的脚本查询主题==

你可以使用下面的命令，查询所有主题的列表。

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --list
```

如果要查询单个主题的详细数据，你可以使用下面的命令。

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --describe --topic 
```

如果describe命令不指定具体的主题名称，那么Kafka默认会返回所有“可见”主题的详细数据给你。

**这里的“可见”，是指发起这个命令的用户能够看到的Kafka主题**。这和前面说到主题创建时，使用 --zookeeper和 --bootstrap-server的区别是一样的。如果指定了 --bootstrap-server，那么这条命令就会受到安全认证体系的约束，即对命令发起者进行权限验证，然后返回它能看到的主题。否则，如果指定 --zookeeper参数，那么默认会返回集群中所有的主题详细数据。基于这些原因，我建议你最好统一使用 --bootstrap-server连接参数。

==3、Kafka的主题修改，一起有五处可以修改的地方==

**1.修改主题分区。**

其实就是增加分区，目前Kafka不允许减少某个主题的分区数。你可以使用kafka-topics脚本，结合 --alter参数来增加某个主题的分区数，命令如下：

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --alter --topic  --partitions <新分区数>
```

这里要注意的是，你指定的分区数一定要比原有分区数大，否则Kafka会抛出InvalidPartitionsException异常。

**2.修改主题级别参数**。

在主题创建之后，我们可以使用kafka-configs脚本修改对应的参数。

假设我们要设置主题级别参数max.message.bytes，那么命令如下：

```
bin/kafka-configs.sh --zookeeper zookeeper_host:port --entity-type topics --entity-name  --alter --add-config max.message.bytes=10485760
```

也许你会觉得奇怪，为什么这个脚本就要指定 --zookeeper，而不是 --bootstrap-server呢？其实，这个脚本也能指定 --bootstrap-server参数，只是它是用来设置动态参数的。在专栏后面，我会详细介绍什么是动态参数，以及动态参数都有哪些。现在，你只需要了解设置常规的主题级别参数，还是使用 --zookeeper。

**3.变更副本数。**

使用自带的kafka-reassign-partitions脚本，帮助我们增加主题的副本数。这里先留个悬念，稍后我会拿Kafka内部主题__consumer_offsets来演示如何增加主题副本数。

**4.修改主题限速。**

这里主要是指设置Leader副本和Follower副本使用的带宽。有时候，我们想要让某个主题的副本在执行副本同步机制时，不要消耗过多的带宽。Kafka提供了这样的功能。我来举个例子。假设我有个主题，名为test，我想让该主题各个分区的Leader副本和Follower副本在处理副本同步时，不得占用超过100MBps的带宽。注意是大写B，即每秒不超过100MB。那么，我们应该怎么设置呢？

要达到这个目的，我们必须先设置Broker端参数leader.replication.throttled.rate和follower.replication.throttled.rate，命令如下：

```
bin/kafka-configs.sh --zookeeper zookeeper_host:port --alter --add-config 'leader.replication.throttled.rate=104857600,follower.replication.throttled.rate=104857600' --entity-type brokers --entity-name 0
```

这条命令结尾处的 --entity-name就是Broker ID。倘若该主题的副本分别在0、1、2、3多个Broker上，那么你还要依次为Broker 1、2、3执行这条命令。

设置好这个参数之后，我们还需要为该主题设置要限速的副本。在这个例子中，我们想要为所有副本都设置限速，因此统一使用通配符*来表示，命令如下：

```
bin/kafka-configs.sh --zookeeper zookeeper_host:port --alter --add-config 'leader.replication.throttled.replicas=*,follower.replication.throttled.replicas=*' --entity-type topics --entity-name test
```

**5.主题分区迁移。**

同样是使用kafka-reassign-partitions脚本，对主题各个分区的副本进行“手术”般的调整，比如把某些分区批量迁移到其他Broker上。这种变更比较复杂，我会在专栏后面专门和你分享如何做主题的分区迁移。

==最后，我们来聊聊如何删除主题。==

命令很简单，我直接分享给你。

```
bin/kafka-topics.sh --bootstrap-server broker_host:port --delete  --topic 
```

删除主题的命令并不复杂，关键是删除操作是异步的，执行完这条命令不代表主题立即就被删除了。它仅仅是被标记成“已删除”状态而已。Kafka会在后台默默地开启主题删除操作。因此，通常情况下，你都需要耐心地等待一段时间。



### 5.1.2 特殊主题的管理与运维（/）

说完了日常的主题管理操作，我们来聊聊Kafka内部主题__consumer_offsets和__transaction_state。前者你可能已经很熟悉了，后者是Kafka支持事务新引入的。如果在你的生产环境中，你看到很多带有__consumer_offsets和__transaction_state前缀的子目录，不用惊慌，这是正常的。这两个内部主题默认都有50个分区，因此，分区子目录会非常得多。

关于这两个内部主题，我的建议是不要手动创建或修改它们，还是让Kafka自动帮我们创建好了。不过这里有个比较隐晦的问题，那就是__consumer_offsets的副本数问题。

在Kafka 0.11之前，当Kafka自动创建该主题时，它会综合考虑当前运行的Broker台数和Broker端参数offsets.topic.replication.factor值，然后取两者的较小值作为该主题的副本数，但这就违背了用户设置offsets.topic.replication.factor的初衷。这正是很多用户感到困扰的地方：我的集群中有100台Broker，offsets.topic.replication.factor也设成了3，为什么我的__consumer_offsets主题只有1个副本？其实，这就是因为这个主题是在只有一台Broker启动时被创建的。

在0.11版本之后，社区修正了这个问题。也就是说，0.11之后，Kafka会严格遵守offsets.topic.replication.factor值。如果当前运行的Broker数量小于offsets.topic.replication.factor值，Kafka会创建主题失败，并显式抛出异常。

那么，如果该主题的副本值已经是1了，我们能否把它增加到3呢？当然可以。我们来看一下具体的方法。

第1步是创建一个json文件，显式提供50个分区对应的副本数。注意，replicas中的3台Broker排列顺序不同，目的是将Leader副本均匀地分散在Broker上。该文件具体格式如下：

```
{"version":1, "partitions":[
 {"topic":"__consumer_offsets","partition":0,"replicas":[0,1,2]}, 
  {"topic":"__consumer_offsets","partition":1,"replicas":[0,2,1]},
  {"topic":"__consumer_offsets","partition":2,"replicas":[1,0,2]},
  {"topic":"__consumer_offsets","partition":3,"replicas":[1,2,0]},
  ...
  {"topic":"__consumer_offsets","partition":49,"replicas":[0,1,2]}
]}`
```

第2步是执行kafka-reassign-partitions脚本，命令如下：

```
bin/kafka-reassign-partitions.sh --zookeeper zookeeper_host:port --reassignment-json-file reassign.json --execute
```

除了修改内部主题，我们可能还想查看这些内部主题的消息内容。特别是对于__consumer_offsets而言，由于它保存了消费者组的位移数据，有时候直接查看该主题消息是很方便的事情。下面的命令可以帮助我们直接查看消费者组提交的位移数据。

```
bin/kafka-console-consumer.sh --bootstrap-server kafka_host:port --topic __consumer_offsets --formatter "kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter" --from-beginning
```

除了查看位移提交数据，我们还可以直接读取该主题消息，查看消费者组的状态信息。

```
bin/kafka-console-consumer.sh --bootstrap-server kafka_host:port --topic __consumer_offsets --formatter "kafka.coordinator.group.GroupMetadataManager$GroupMetadataMessageFormatter" --from-beginning
```

对于内部主题__transaction_state而言，方法是相同的。你只需要指定kafka.coordinator.transaction.TransactionLog$TransactionLogMessageFormatter即可。



## 5.2 Kafka动态配置了解下？



### 5.2.1 什么是动态Broker参数配置？

所谓动态，就是指修改参数值后，无需重启Broker就能立即生效，而之前在server.properties中配置的参数则称为静态参数（Static Configs）。显然，动态调整参数值而无需重启服务，是非常实用的功能。



<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/22ef35547f9bddb8d56bdb350fd52f74.jpg" alt="img" style="zoom:25%;" />



## 5.3 重设消费者组位移

### 5.3.1 为什么要重设消费者组位移？

我们知道，Kafka和传统的消息引擎在设计上是有很大区别的，其中一个比较显著的区别就是，Kafka的消费者读取消息是可以重演的（replayable）。

像RabbitMQ或ActiveMQ这样的传统消息中间件，它们处理和响应消息的方式是破坏性的（destructive），即一旦消息被成功处理，就会被从Broker上删除。

反观Kafka，由于它是基于日志结构（log-based）的消息引擎，消费者在消费消息时，仅仅是从磁盘文件上读取数据而已，是只读的操作，因此消费者不会删除消息数据。同时，由于位移数据是由消费者控制的，因此它能够很容易地修改位移的值，实现重复消费历史数据的功能。

### 5.3.2 重设位移策略

不论是哪种设置方式，重设位移大致可以从两个维度来进行。

1. 位移维度。这是指根据位移值来重设。也就是说，直接把消费者的位移值重设成我们给定的位移值。
2. 时间维度。我们可以给定一个时间，让消费者把位移调整成大于该时间的最小位移；也可以给出一段时间间隔，比如30分钟前，然后让消费者直接将位移调回30分钟之前的位移值。

下面的这张表格罗列了7种重设策略。接下来，我来详细解释下这些策略。

![img](https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/eb469122e5af2c9f6baebb173b56bed5.jpeg)



### 5.3.3 消费者API方式设置

首先，我们来看看如何通过API的方式来重设位移。我主要以Java API为例进行演示。如果你使用的是其他语言，方法应该是类似的，不过你要参考具体的API文档。

通过Java API的方式来重设位移，你需要调用KafkaConsumer的seek方法，或者是它的变种方法seekToBeginning和seekToEnd。我们来看下它们的方法签名。

```
void seek(TopicPartition partition, long offset);
void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);
void seekToBeginning(Collection partitions);
void seekToEnd(Collection partitions);
```

根据方法的定义，我们可以知道，每次调用seek方法只能重设一个分区的位移。OffsetAndMetadata类是一个封装了Long型的位移和自定义元数据的复合类，只是一般情况下，自定义元数据为空，因此你基本上可以认为这个类表征的主要是消息的位移值。seek的变种方法seekToBeginning和seekToEnd则拥有一次重设多个分区的能力。我们在调用它们时，可以一次性传入多个主题分区。

> 好了，有了这些方法，我们就可以逐一地实现上面提到的7种策略了。我们先来看Earliest策略的实现方式，代码如下：
>
> ```
> Properties consumerProperties = new Properties();
> consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
> consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
> consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
> consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
> consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
> consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
> 
> String topic = "test";  // 要重设位移的Kafka主题 
> try (final KafkaConsumer consumer = 
> 	new KafkaConsumer<>(consumerProperties)) {
>          consumer.subscribe(Collections.singleton(topic));
>          consumer.poll(0);
>          consumer.seekToBeginning(
> 	consumer.partitionsFor(topic).stream().map(partitionInfo ->          
> 	new TopicPartition(topic, partitionInfo.partition()))
> 	.collect(Collectors.toList()));
> } 
> ```
>
> 这段代码中有几个比较关键的部分，你需要注意一下。
>
> 1. 你要创建的消费者程序，要禁止自动提交位移。
> 2. 组ID要设置成你要重设的消费者组的组ID。
> 3. 调用seekToBeginning方法时，需要一次性构造主题的所有分区对象。
> 4. 最重要的是，一定要调用带长整型的poll方法，而不要调用consumer.poll(Duration.ofSecond(0))。
>
> 虽然社区已经不推荐使用poll(long)了，但短期内应该不会移除它，所以你可以放心使用。另外，为了避免重复，在后面的实例中，我只给出最关键的代码。
>
> Latest策略和Earliest是类似的，我们只需要使用seekToEnd方法即可，如下面的代码所示：
>
> ```
> consumer.seekToEnd(
> 	consumer.partitionsFor(topic).stream().map(partitionInfo ->          
> 	new TopicPartition(topic, partitionInfo.partition()))
> 	.collect(Collectors.toList()));
> ```
>
> 实现Current策略的方法很简单，我们需要借助KafkaConsumer的committed方法来获取当前提交的最新位移，代码如下：
>
> ```
> consumer.partitionsFor(topic).stream().map(info -> 
> 	new TopicPartition(topic, info.partition()))
> 	.forEach(tp -> {
> 	long committedOffset = consumer.committed(tp).offset();
> 	consumer.seek(tp, committedOffset);
> });
> ```
>
> 这段代码首先调用partitionsFor方法获取给定主题的所有分区，然后依次获取对应分区上的已提交位移，最后通过seek方法重设位移到已提交位移处。
>
> 如果要实现Specified-Offset策略，直接调用seek方法即可，如下所示：
>
> ```
> long targetOffset = 1234L;
> for (PartitionInfo info : consumer.partitionsFor(topic)) {
> 	TopicPartition tp = new TopicPartition(topic, info.partition());
> 	consumer.seek(tp, targetOffset);
> }
> ```
>
> 这次我没有使用Java 8 Streams的写法，如果你不熟悉Lambda表达式以及Java 8的Streams，这种写法可能更加符合你的习惯。
>
> 接下来我们来实现Shift-By-N策略，主体代码逻辑如下：
>
> ```
> for (PartitionInfo info : consumer.partitionsFor(topic)) {
>          TopicPartition tp = new TopicPartition(topic, info.partition());
> 	// 假设向前跳123条消息
>          long targetOffset = consumer.committed(tp).offset() + 123L; 
>          consumer.seek(tp, targetOffset);
> }
> ```
>
> 如果要实现DateTime策略，我们需要借助另一个方法：**KafkaConsumer.** **offsetsForTimes方法**。假设我们要重设位移到2019年6月20日晚上8点，那么具体代码如下：
>
> ```
> long ts = LocalDateTime.of(
> 	2019, 6, 20, 20, 0).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
> Map timeToSearch = 
>          consumer.partitionsFor(topic).stream().map(info -> 
> 	new TopicPartition(topic, info.partition()))
> 	.collect(Collectors.toMap(Function.identity(), tp -> ts));
> 
> for (Map.Entry entry : 
> 	consumer.offsetsForTimes(timeToSearch).entrySet()) {
> consumer.seek(entry.getKey(), entry.getValue().offset());
> }
> ```
>
> 这段代码构造了LocalDateTime实例，然后利用它去查找对应的位移值，最后调用seek，实现了重设位移。
>
> 最后，我来给出实现Duration策略的代码。假设我们要将位移调回30分钟前，那么代码如下：
>
> ```
> Map timeToSearch = consumer.partitionsFor(topic).stream()
>          .map(info -> new TopicPartition(topic, info.partition()))
>          .collect(Collectors.toMap(Function.identity(), tp -> System.currentTimeMillis() - 30 * 1000  * 60));
> 
> for (Map.Entry entry : 
>          consumer.offsetsForTimes(timeToSearch).entrySet()) {
>          consumer.seek(entry.getKey(), entry.getValue().offset());
> }
> ```
>
> **总之，使用Java API的方式来实现重设策略的主要入口方法，就是seek方法**。



### 5.3.4 命令行方式设置

位移重设还有另一个重要的途径：**通过kafka-consumer-groups脚本**。需要注意的是，这个功能是在Kafka 0.11版本中新引入的。这就是说，如果你使用的Kafka是0.11版本之前的，那么你只能使用API的方式来重设位移。

比起API的方式，用命令行重设位移要简单得多。针对我们刚刚讲过的7种策略，有7个对应的参数。下面我来一一给出实例。

Earliest策略直接指定**–to-earliest**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-earliest –execute
```

Latest策略直接指定**–to-latest**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-latest --execute
```

Current策略直接指定**–to-current**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-current --execute
```

Specified-Offset策略直接指定**–to-offset**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-offset  --execute
```

Shift-By-N策略直接指定**–shift-by N**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --shift-by  --execute
```

DateTime策略直接指定**–to-datetime**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --to-datetime 2019-06-20T20:00:00.000 --execute
```

最后是实现Duration策略，我们直接指定**–by-duration**。

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --by-duration PT0H30M0S --execute
```



### 小结

<img src="https://java-baguwen.oss-cn-chengdu.aliyuncs.com/images/98a7d3f9b0d3050947772d8cd2c4caf3.jpg" alt="img" style="zoom: 25%;" />



