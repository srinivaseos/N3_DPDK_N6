#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <jansson.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>

#include<rte_eal.h>
#include<rte_lcore.h>
#include<rte_per_lcore.h> 
#include<rte_ethdev.h>
#include<rte_mbuf.h>
#include <rte_mbuf.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <rte_ether.h>
#include <rte_cycles.h>
#include <pcap/pcap.h>


#define NUM_MBUFS  4096
#define MBUF_CACHE_SIZE 256
#define BRUST_SIZE 32
#define APP_LOG_MSG_SIZE 400
#define MAX_LOG_SIZE 10485760 // 10 Megabytes limit
typedef struct  dpdk_perf_stats
{
	uint64_t  total_received;
	uint64_t  total_dequeued;
	uint64_t  total_enqueued;
	uint64_t  total_sent;
	uint64_t  total_proceed;
	uint64_t  total_dropped;
	uint64_t  tx_dropped;
	uint64_t  p_dropped;

	uint64_t  last_received;
	uint64_t  last_dequeued;
	uint64_t  last_enqueued;
	uint64_t  last_sent;
	uint64_t  last_proceed;
	uint64_t  last_dropped;
	uint64_t  la_tx_dropped;
	uint64_t  la_p_dropped;
	uint64_t arp_request_received;
	uint64_t arp_responce_sent;


}dpdk_perf_stats_t;


typedef struct app_details
{
	uint16_t port;
	
	struct rte_ether_addr mac_addr;
    struct rte_ring  *ring_buffer;
	struct rte_mempool *mbuf_pool;
	struct app_details *next;
	
}app_details_t;

typedef struct dpdk_app
{
	int index;
	uint16_t portid;
	char 	port[20];
	char 	tx_port[20];
	uint32_t ipv4;
	uint8_t macaddr[8];
	uint8_t tx_macaddr[6]; 
	uint8_t src_macaddr[8];
	uint8_t rx;
	uint8_t tx;
	
	uint8_t rx_queue[5];
	uint8_t tx_queue[5];
	uint8_t worker_queue[5];
	uint8_t rx_queueid;
	uint8_t tx_queueid;
	uint8_t	 workerid;
	
	uint16_t mbufsize;
	uint16_t mbufcachesize;
	uint8_t brustsize;
	uint16_t rxdesc;
	uint16_t txdesc;
	uint16_t mtusize;
	char 	txport[20];
	uint32_t Destip;
	pcap_dumper_t *dumper;
	struct rte_ether_addr mac_addr;
    struct rte_ring  *rxring_buffer[5]; 
	struct rte_ring  *txring_buffer[5];
	struct rte_mempool *mbuf_pool[5];
	//struct rte_mbuf *pkt_mbuf[BRUST_SIZE];

	struct app_details *next;
	uint8_t Enablelogs;
	int p_fd;
	int s_fd;
	char folder[10];
	char path_folder[15];
	char stats_folder[15];
	pthread_mutex_t perf_fd;
	dpdk_perf_stats_t dp_stats; 
	uint8_t TraceEnable;
	struct dpdk_app *Next;
	struct dpdk_app *tx_interface;
}dpdk_app_t;

dpdk_app_t * dp=NULL;


typedef struct app_info
{
	app_details_t * app_res;
	app_details_t * app_req;
}app_info_t;

app_info_t * arp_info=NULL;

//struct rte_eth_conf port_conf = {
	// Reset the structure completely to avoid garbage stack memory values
	//memset(&port_conf, 0, sizeof(struct rte_eth_conf));

	// Basic RX Mode configurations
	//port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE; // Disable RSS (Highly compatible)

	// Basic TX Mode configurations 
	//port_conf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;

	// Loopback Configuration Option
	// WARNING: Set to 1 ONLY if your hardware documentation explicitly supports it.
	// If using a physical loopback cable, keep this set to 0.
	//port_conf.lpbk_mode = 0; 
   // .lpbk_mode = 1, // Enable internal hardware loopback if supported by your PMD
    //.rxmode = {
       // .mq_mode = RTE_ETH_MQ_RX_RSS, // for single queue RX/Tx
        
    //},
	//.txmode = {
        //.mq_mode = RTE_ETH_MQ_TX_NONE,
       // .mq_mode = RTE_ETH_MQ_TX_NONE,
    //},
	//.rx_adv_conf = {
       // .rss_conf = {
            //.rss_hf = RTE_ETH_RSS_IP | RTE_ETH_RSS_TCP | RTE_ETH_RSS_UDP,
       // },
   // },
   //.rx_adv_conf.rss_conf.rss_hf	= RTE_ETH_RSS_IP,
   //.rx_adv_conf.rss_conf.rss_hf = dev_info.flow_type_rss_offloads,
	
//};
int file_status(const char *name)
{
    struct stat buf; 
    if(stat(name,&buf) == 0)
    {
        printf("created file name=%s|%s|%d\n",name,__FILE__,__LINE__);
        return 0;
    }
   return 1;
}

void time_stamp_buffer(char *time_buffer,int b_size)
{
	memset(time_buffer,0,b_size);
	time_t raw_time = time(NULL);
	struct tm *timestamp = localtime(&raw_time); 
	//sprintf(buffer,"%d_%02d_%02d_%02d_%02d",timestamp->tm_year + 1900,timestamp->tm_mon +1,timestamp->tm_hour,timestamp->tm_min,timestamp->tm_sec);
    sprintf(time_buffer,"%02d_%02d_%d_%02d_%02d_%02d",timestamp->tm_mday,timestamp->tm_mon +1,timestamp->tm_year + 1900,timestamp->tm_hour,timestamp->tm_min,timestamp->tm_sec);
}

int folder_create()
{
	int sts=0;
    int log_ifp =0;
    int mkd_ifp =0;
    int fd =0;
	pthread_mutex_init(&dp->perf_fd,NULL);
	
	
	char filename[200];
	char time_buffer[40];
    if(file_status(dp->folder))
    {
       // printf("file_status = %d|%d\n",file_status(name),__LINE__);
       // ifp = S_IRWXU | S_IRGRP |S_IWGRP | S_IROTH;
        mkd_ifp = S_IRWXU | S_IRGRP| S_IXGRP  |S_IROTH | S_IXOTH;
        sts = mkdir(dp->folder,mkd_ifp);

        if(sts < 0 )
        {
            perror("error opening file");
            printf("File creation failed name=%s errno=%d str=%s|%s|%d\n",dp->folder,errno,strerror(errno),__FILE__,__LINE__);
            exit(0);
        }
        else{
            printf("logs folder created=%d,name=%s|%s|%d\n",sts,dp->folder,__FILE__,__LINE__);
        }
    }
    else{
        perror("folder is already created");
       printf("file_status = %d name=%s|%s|%d\n",sts,dp->folder,__FILE__,__LINE__);
    }
	memset(filename,0,sizeof(filename));
	time_stamp_buffer(time_buffer,sizeof(time_buffer));
	sprintf(filename,"%s/%s_%s.log",dp->folder,dp->path_folder,time_buffer);
    log_ifp = S_IRUSR | S_IWUSR |S_IRGRP| S_IROTH  ;
    dp->p_fd = creat(filename,log_ifp);
    if(fd < 0 )
    {
    	perror("error opening file");
        printf("File creation failed name=%s errno=%d str=%s|%s|%d\n",filename,errno,strerror(errno),__FILE__,__LINE__);
        exit(0);
    }
    else
	{ 
        printf("logs folder creared=%d,name=%s|%s|%d\n", dp->p_fd,filename,__FILE__,__LINE__);
        //  fd = open(perf_name,log_ifp);
        ssize_t bytes_written = write( dp->p_fd,"msgs logs started\n",18);

        if (bytes_written == -1) {
        perror("Write failed");
		printf("Write failed %s|%d\n",__FILE__,__LINE__);
        close( dp->p_fd);
        return 1;
        }

            
            
    }
	memset(filename,0,sizeof(filename));
	
	sprintf(filename,"%s/%s_%s.log",dp->folder,dp->stats_folder,time_buffer);
	dp->s_fd= creat(filename,log_ifp);
     if(fd < 0 )
        {
            perror("error opening file");
            printf("File creation failed name=%s errno=%d str=%s|%s|%d\n",filename,errno,strerror(errno),__FILE__,__LINE__);
            exit(0);
        }
        else{
            
            printf("logs folder created=%d,name=%s|%s|%d\n",dp->s_fd,filename,__FILE__,__LINE__);
          //  fd = open(perf_name,log_ifp);
             ssize_t bytes_written = write(dp->s_fd,"performance stats logs",22);

            if (bytes_written == -1) {
            perror("Write failed");
			printf("Write failed %s|%d\n",__FILE__,__LINE__);
            close(dp->s_fd);
            return 1;
             }

            
            
        }
         //close(dp->s_fd);
         printf("\n");
    
    return 0;
}

int create_folder(int fd_id,char *m_buffer,int size)
{
	char time_buffer[40];
	time_stamp_buffer(time_buffer,sizeof(time_buffer));;
		
	
	char filename[200];
	memset(filename,0,sizeof(filename));
	int log_ifp = S_IRUSR | S_IWUSR |S_IRGRP| S_IROTH | O_APPEND ;	
	if(fd_id == dp->p_fd)
	{
		sprintf(filename,"%s/%s_%s.log",dp->folder,dp->path_folder,time_buffer);
	
		dp->p_fd = creat(filename,log_ifp);
		if(dp->p_fd < 0 )
		{
			perror("error opening file");
			printf("File creation failed name=%s errno=%d str=%s|%s|%d\n",filename,errno,strerror(errno),__FILE__,__LINE__);
			exit(0);
		}
		else
		{
			//printf("logs folder creating=%d,name=%s|%s|%d\n", dp->p_fd,filename,__FILE__,__LINE__);
			
			ssize_t bytes_written = write( dp->p_fd,m_buffer,size);

			if (bytes_written == -1) {
			perror("Write failed");
			printf("Write failed %s|%d\n",__FILE__,__LINE__);
			close( dp->p_fd);
			return 0;
				}  
			ssize_t written = write(fd_id,"\n",1);
			if (written == -1) {
			perror("Write failed");
			printf("written=%ld|%s|%d\n",written,__FILE__,__LINE__);
			close( fd_id);
				}
			return dp->p_fd;  
		}
	}
	else if(fd_id == dp->s_fd)
	{
		sprintf(filename,"%s/%s_%s.log",dp->folder,dp->stats_folder,time_buffer);
	
		dp->s_fd = creat(filename,log_ifp);
		if(dp->s_fd < 0 )
		{
			perror("error opening file");
			printf("File creation failed name=%s errno=%d str=%s|%s|%d\n",filename,errno,strerror(errno),__FILE__,__LINE__);
			exit(0);
		}
		else
		{
			//printf("logs folder creating=%d,name=%s|%s|%d\n", dp->s_fd,filename,__FILE__,__LINE__);
			
			ssize_t bytes_written = write( dp->s_fd,m_buffer,size);

			if (bytes_written == -1) {
			perror("Write failed");
			printf("Write failed %s|%d\n",__FILE__,__LINE__);
			close( dp->s_fd);
			return 0;
				}   
			ssize_t written = write(fd_id,"\n",1);
			if (written == -1) {
			perror("Write failed");
			printf("written=%ld|%s|%d\n",written,__FILE__,__LINE__);
			close( fd_id);
			}
			return  dp->s_fd;
		}
	}
	else{
		printf("no file created fd_id=%d| %s|%d\n",fd_id,__FILE__,__LINE__);
	}
}


void dpdk_perf_stats_log(int fd_id,const char *format,...)
{
	
	if(dp->Enablelogs == 1)
	{
		//printf("[DEBUG BEFORE] fd_id = %d\n", fd);
		
		if(!format || (APP_LOG_MSG_SIZE < (int)strlen(format) ))
		{
			printf("no format and file is more than define size");
			return;
		}

		pthread_mutex_lock(&dp->perf_fd);
		va_list arg;
		va_start(arg,format);

		char m_buffer[APP_LOG_MSG_SIZE];

		//printf("[DEBUG BEFORE] fd_id = %d\n", fd_id);

		vsnprintf(m_buffer,APP_LOG_MSG_SIZE,format,arg);
		va_end(arg);

		//printf("[DEBUG AFTER] fd_id = %d\n", fd_id);
		struct stat file_info;
		if(fd_id < 0)
		{
	
			perror("Error opening file");
			return;
			
		}
		if(fstat(fd_id,&file_info) < 0)
		{
			perror("file_size");
			printf("Write failed fd_id=%d size=%ld %s|%d\n",fd_id,file_info.st_size,__FILE__,__LINE__); 
			close(fd_id);
			fd_id = -1;
			pthread_mutex_unlock(&dp->perf_fd);
			return;
		}
		else
		{
			//printf("Writing fd_id=%d size=%ld %s|%d\n",fd_id,file_info.st_size,__FILE__,__LINE__); 
		}
		
		unsigned long filesize = file_info.st_size;
		if(filesize >= MAX_LOG_SIZE )
		{
			long bytes = filesize;
			double kilobytes = (double)bytes / 1024.0;
			double megabytes = kilobytes / 1024.0;

			//printf("[Log Status] Active FD: %d | Current Size: %ld bytes (%.2f KB | %.2f MB)\n", fd_id, bytes, kilobytes, megabytes);
			close(fd_id);
			//printf("creating another floder\n");
			fd_id = create_folder(fd_id,m_buffer,strlen(m_buffer));
		}
		else
		{
			ssize_t bytes_written = write( fd_id,m_buffer,strlen(m_buffer));
									//write(fd_id,"\n",1);
			if (bytes_written <= 0) {
			perror("Write failed");
			printf("bytes_written=%ld|%s|%d\n",bytes_written,__FILE__,__LINE__);
			close( fd_id);
			pthread_mutex_unlock(&dp->perf_fd);
			return ;
				}
			if(fd_id == dp->p_fd)
			{
				//printf("bytes_written=%ld|%s|%d\n",bytes_written,__FILE__,__LINE__);
			}
			ssize_t written = write(fd_id,"\n",1);
			if (written <= 0) {
			perror("Write failed");
			printf("written=%ld|%s|%d\n",written,__FILE__,__LINE__);
			close( fd_id);
			pthread_mutex_unlock(&dp->perf_fd);
			return ;
				}
			//write(fd_id,"\n",1);
				
		}
		pthread_mutex_unlock(&dp->perf_fd);
	}
}

void dpdk_log_thread(void *arg)
{
	dpdk_perf_stats_log(dp->p_fd,"------------APP LOG STARTED------------"); 
}

int dpdk_open_dumper_packet()
{
	pcap_t *handle = pcap_open_dead_with_tstamp_precision(DLT_EN10MB, 2000, PCAP_TSTAMP_PRECISION_NANO);
	time_t raw_time = time(NULL);
	struct tm *timestamp = localtime(&raw_time);
	char buffer[40];
	//sprintf(buffer,"%d_%02d_%02d_%02d_%02d",timestamp->tm_year + 1900,timestamp->tm_mon +1,timestamp->tm_hour,timestamp->tm_min,timestamp->tm_sec);
	sprintf(buffer,"%02d_%02d_%d_%02d_%02d_%02d",timestamp->tm_mday,timestamp->tm_mon +1,timestamp->tm_year + 1900,timestamp->tm_hour,timestamp->tm_min,timestamp->tm_sec);
	
	char filename[200];
	memset(filename,0,sizeof(filename));
	sprintf(filename,"GTP_to_packet_%s.pcap",buffer);
	dp->dumper =pcap_dump_open(handle,filename);
	return 0;
}

void dpdk_write_open_pcap(pcap_dumper_t *dumper,struct rte_mbuf *m)
{
	if(dp->TraceEnable == 1)
	{
		if(m && dumper)
		{
			uint8_t temp_data[2000];

			struct pcap_pkthdr header;
			gettimeofday(&header.ts,NULL);
			header.len = rte_pktmbuf_pkt_len(m);
			header.caplen = RTE_MIN( header.len, 2000); //rte_pktmbuf_data_len(pkts_burst0[i]);
			//const u_char *packet_bytes = rte_pktmbuf_mtod(m, const u_char *);
			
			pcap_dump((u_char *)dumper, &header,rte_pktmbuf_read(m,0,header.caplen,temp_data)); 
		}
		else
		{
			printf("failed to write=%p|%d\n",m,__LINE__);
		}
	}
}

void close_folder()
{
	if(dp->Enablelogs == 1)
	{
	 	close(dp->p_fd);
	}
}

void dpdk_close_dumper_packet(pcap_dumper_t *dumper)
{
	if(dp->TraceEnable == 1)
	{
		if(dumper)
		{
			pcap_dump_close(dumper);
		}
	}
}

void arp_send_request(dpdk_app_t *interface)
{
	dpdk_perf_stats_log(dp->p_fd,"arp request sending interface->portid=%d",interface->portid);
	struct rte_mbuf *pkt[1] = {rte_pktmbuf_alloc(interface->mbuf_pool[0])};
	struct rte_mbuf *arp_pkt = pkt[0];
	
	//printf("ether addrs srcip=%u  dstip=%u |%d\n",rte_be_to_cpu_32(interface->ipv4),rte_be_to_cpu_32(interface->Destip),__LINE__);
	
	uint16_t arp_pkt_len = 60; 
	char *data = rte_pktmbuf_append(arp_pkt,arp_pkt_len);
	
	struct rte_ether_hdr *eth= rte_pktmbuf_mtod(arp_pkt,struct rte_ether_hdr *);
	
	eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_ARP);
	uint8_t raw[8] = {0x00,0x0C,0x29,0x25,0x4F,0xE2}; //:00:0C:29:25:4F:E2
	uint8_t raw_dst[8] = {0x00,0x0C,0x29,0x25,0x4F,0xD8};
	eth->src_addr.addr_bytes[0]=raw[0];
	eth->src_addr.addr_bytes[1]=raw[1];
	eth->src_addr.addr_bytes[2]=raw[2];
	eth->src_addr.addr_bytes[3]=raw[3];
	eth->src_addr.addr_bytes[4]=raw[4];
	eth->src_addr.addr_bytes[5]=raw[5];
	
	eth->dst_addr.addr_bytes[0]=0xFF;
	eth->dst_addr.addr_bytes[1]=0xFF;
	eth->dst_addr.addr_bytes[2]=0xFF;
	eth->dst_addr.addr_bytes[3]=0xFF;
	eth->dst_addr.addr_bytes[4]=0xFF;
	eth->dst_addr.addr_bytes[5]=0xFF;
	
	 printf("%d src MAC: %02X:%02X:%02X:%02X:%02X:%02X \n",interface->portid,
										eth->src_addr.addr_bytes[0],eth->src_addr.addr_bytes[1],eth->src_addr.addr_bytes[2],
										eth->src_addr.addr_bytes[3],eth->src_addr.addr_bytes[4],eth->src_addr.addr_bytes[5]);
	
	
	struct rte_arp_hdr *arp = (struct rte_arp_hdr *)(eth + 1); 
	
		arp->arp_hardware = htons(1);
		arp->arp_protocol = htons(RTE_ETHER_TYPE_IPV4);
		arp->arp_hlen     = 6;
		arp->arp_plen	  = 4;
		arp->arp_opcode   = rte_cpu_to_be_16(RTE_ARP_OP_REQUEST);
		
		//arp->arp_protocol = 
		rte_ether_addr_copy(&eth->src_addr,&arp->arp_data.arp_sha);
		rte_ether_addr_copy(&eth->dst_addr,&arp->arp_data.arp_tha);
		
		//char src_ip[INET_ADDRSTRLEN]="192.168.144.55";
		//char dst_ip[INET_ADDRSTRLEN]="192.168.144.22";
		uint32_t src_ip= rte_cpu_to_be_32(3232272439);
		uint32_t dst_ip= rte_cpu_to_be_32(3232272406);
		struct sockaddr_in saddr;
		struct sockaddr_in daddr;
		
		saddr.sin_addr.s_addr =src_ip; 
		daddr.sin_addr.s_addr =dst_ip; 
		//inet_aton(src_ip,&saddr.sin_addr);
		//inet_aton((char *)dst_ip,&daddr.sin_addr);
		// Copy 4 bytes from the socket address into the ARP array
		memcpy(&arp->arp_data.arp_sip, &saddr.sin_addr.s_addr, sizeof(saddr.sin_addr.s_addr));
		memcpy(&arp->arp_data.arp_tip, &daddr.sin_addr.s_addr, sizeof(saddr.sin_addr.s_addr));
		
		char src[INET_ADDRSTRLEN];
		char dst[INET_ADDRSTRLEN];

		inet_ntop(AF_INET,&arp->arp_data.arp_sip,src,sizeof(src));
		inet_ntop(AF_INET,&arp->arp_data.arp_tip,dst,sizeof(dst));

		dpdk_perf_stats_log(dp->p_fd,"arp request source ip=%s destination ip=%s|%s|%d",src,dst,__FILE__,__LINE__);
		
	uint16_t nb_tx=0;
	nb_tx = rte_eth_tx_burst(1,0,pkt,1);
	dpdk_perf_stats_log(dp->p_fd,"create arp packet transmit =%d  | %s %d",nb_tx,__FILE__,__LINE__); 
	for(int i=0;i<nb_tx;i++) 
	{
		dpdk_write_open_pcap(dp->dumper,arp_pkt); 
	}
	rte_pktmbuf_free(arp_pkt);
	
}

void create_gtp_packet(dpdk_app_t *interface)
{
	struct rte_mbuf *pkt[1] = {rte_pktmbuf_alloc(interface->mbuf_pool[0])};
	struct rte_mbuf *create_pkt = pkt[0];
	if(create_pkt == NULL)
	{
		printf("Failed to allocte mempool dpe->mbuf_pool[%p] %d\n",interface->mbuf_pool[0],__LINE__);
	}
	else
	{
		printf("success to allocte mempool dpe->mbuf_pool[%p] %d\n",interface->mbuf_pool[0],__LINE__);
	}
	
	printf("1.data_len=%d pkt_len=%d | %d\n",create_pkt->data_len,create_pkt->pkt_len,__LINE__);
	uint16_t gtp_pkt_len = 14+20+8+8+20+8+100; 
	char *data = rte_pktmbuf_append(create_pkt,gtp_pkt_len);
	create_pkt->ol_flags = RTE_MBUF_F_TX_TUNNEL_GTP | RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_UDP_CKSUM;
	printf("2.data_len=%d pkt_len=%d | %d\n",create_pkt->data_len,create_pkt->pkt_len,__LINE__);
	if(data == NULL)
	{
		rte_pktmbuf_free(create_pkt);
		printf("pkt_mbuf adjacent failed %d\n",__LINE__);
		return;
	}
	
	struct rte_ether_hdr *eth = rte_pktmbuf_mtod(create_pkt,struct rte_ether_hdr*);
	eth->ether_type = 8;
	uint8_t raw_dst[8] = {0x00,0x0C,0x29,0x30,0x47,0xD3}; //00:0C:29:30:47:D3
	uint8_t raw[8] = {0x00,0x0C,0x29,0x25,0x4F,0xD8};
	eth->src_addr.addr_bytes[0]=raw[0];//interface->macaddr[0];
	eth->src_addr.addr_bytes[1]=raw[1];//interface->macaddr[1];
	eth->src_addr.addr_bytes[2]=raw[2];//interface->macaddr[2];
	eth->src_addr.addr_bytes[3]=raw[3];//interface->macaddr[3];
	eth->src_addr.addr_bytes[4]=raw[4];//interface->macaddr[4];
	eth->src_addr.addr_bytes[5]=raw[5];//interface->macaddr[5];
	
	eth->dst_addr.addr_bytes[0]=raw_dst[0];
	eth->dst_addr.addr_bytes[1]=raw_dst[1];
	eth->dst_addr.addr_bytes[2]=raw_dst[2];
	eth->dst_addr.addr_bytes[3]=raw_dst[3];
	eth->dst_addr.addr_bytes[4]=raw_dst[4];
	eth->dst_addr.addr_bytes[5]=raw_dst[5];
	
	//outer ipv4
	
	struct rte_ipv4_hdr *o_ipv4 = rte_pktmbuf_mtod_offset(create_pkt,struct rte_ipv4_hdr*,sizeof(struct rte_ether_hdr));
	
	o_ipv4->version=0;
	o_ipv4->src_addr=0;
	o_ipv4->dst_addr=0;
	
	o_ipv4->version=4;
	o_ipv4->ihl =5;
	//o_ipv4->total_length =rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr));
	o_ipv4->packet_id = gtp_pkt_len;
	//o_ipv4->flags =0;
	//o_ipv4->fragment_offset.flags = 0;
	o_ipv4->fragment_offset =rte_cpu_to_be_16(0x4000);//(RTE_IPV4_HDR_DF_FLAG);
	o_ipv4->time_to_live=64;
	o_ipv4->next_proto_id = 17;
	//uint32_t src_addr=3232272397; //192.168.144.13
	o_ipv4->src_addr= rte_cpu_to_be_32(3232272437);//interface->ipv4;
	o_ipv4->dst_addr= rte_cpu_to_be_32(3232272405);//interface->dst_ipv4;//
	o_ipv4->total_length =rte_cpu_to_be_16((create_pkt->pkt_len)-14);
	o_ipv4->hdr_checksum = 0;
	o_ipv4->hdr_checksum = rte_ipv4_cksum(o_ipv4);
	
	
	//outer udp protcol
	struct rte_udp_hdr *o_udp = rte_pktmbuf_mtod_offset(create_pkt,struct rte_udp_hdr*,sizeof(struct rte_ipv4_hdr)+sizeof(struct rte_ether_hdr));
	//struct rte_udp_hdr *o_udp = (struct rte_udp_hdr*)(o_ipv4 +1);
	
	o_udp->src_port =rte_cpu_to_be_16(2152);
	o_udp->dst_port = rte_cpu_to_be_16(2152);
	
	
	o_udp->dgram_len = rte_cpu_to_be_16((create_pkt->pkt_len)-14-20);
	o_udp->dgram_cksum =0;
	
	//GTP-U layer
	
	struct rte_gtp_hdr *gtp = rte_pktmbuf_mtod_offset(create_pkt,struct rte_gtp_hdr*,sizeof(struct rte_ipv4_hdr)+sizeof(struct rte_ether_hdr)+sizeof(struct rte_udp_hdr));
	
	gtp->gtp_hdr_info =48;
	gtp->msg_type = 255;
	gtp->plen =rte_cpu_to_be_16((create_pkt->pkt_len)-14-20-8);
	gtp->teid = rte_cpu_to_be_32(0x1);
	
	//printf("version=%d src_addr=%u |%d\n",o_ipv4->version,o_ipv4->src_addr,__LINE__);
	
	//INNER IPV4
	struct rte_ipv4_hdr *i_ipv4 = (struct rte_ipv4_hdr *)(gtp+1);
	
	i_ipv4->version = 4;
	i_ipv4->ihl		= 5;
	i_ipv4->type_of_service = 0;
	i_ipv4->total_length = rte_cpu_to_be_16((create_pkt->pkt_len)-14-20-8-8);
	i_ipv4->packet_id =gtp_pkt_len+1;
	i_ipv4->fragment_offset =rte_cpu_to_be_16(0x4000);
	i_ipv4->time_to_live = 64;
	i_ipv4->next_proto_id = 17;
	i_ipv4->src_addr = rte_cpu_to_be_32(2899050753);
	i_ipv4->dst_addr = rte_cpu_to_be_32(2899050754);
	i_ipv4->hdr_checksum = 0;
	i_ipv4->hdr_checksum = rte_ipv4_cksum(i_ipv4);
	
	//INNER UDP
	struct rte_udp_hdr *i_udp =(struct rte_udp_hdr *)(i_ipv4 +1);
	
	i_udp->src_port = rte_cpu_to_be_16(6565);
	i_udp->dst_port =rte_cpu_to_be_16(530);
	i_udp->dgram_len =rte_cpu_to_be_16((create_pkt->pkt_len)-14-20-8-8-20);
	
	i_udp->dgram_cksum = 0;
	i_udp->dgram_cksum = rte_ipv4_udptcp_cksum(i_ipv4,i_udp);
	
	o_udp->dgram_cksum = rte_ipv4_udptcp_cksum(o_ipv4,o_udp);
	
	
	
	uint16_t nb_tx=0;
	nb_tx = rte_eth_tx_burst(0,0,&create_pkt,1);
	dpdk_perf_stats_log(dp->p_fd,"create gtp packet transmit =%d  | %s %d\n",nb_tx,__FILE__,__LINE__); 
	for(int i=0;i<nb_tx;i++) 
	{
		dpdk_write_open_pcap(dp->dumper,create_pkt); 
	}
	rte_pktmbuf_free(create_pkt);
	//exit(0);
}

int arp_pkt_code =0;
int gtp_pkt_code =0;
int gi_pkt_code =0;

int arp_packet_responce(dpdk_app_t *interface ,struct rte_mbuf *m,uint16_t *arp_recvie,uint16_t *arp_sent,int *req_sent_code)
{
	uint16_t arp_r =0 ;
	uint16_t arp_s =0;
	struct rte_ether_hdr *eth= rte_pktmbuf_mtod(m,struct rte_ether_hdr *);
	struct rte_arp_hdr *arp = (struct rte_arp_hdr *)(eth + 1); 
	if(arp->arp_opcode == rte_be_to_cpu_16(RTE_ARP_OP_REQUEST))
	{
		char src_ip[INET_ADDRSTRLEN];
		char dst_ip[INET_ADDRSTRLEN];

		inet_ntop(AF_INET,&arp->arp_data.arp_sip,src_ip,sizeof(src_ip));
		inet_ntop(AF_INET,&arp->arp_data.arp_tip,dst_ip,sizeof(dst_ip));
		dpdk_perf_stats_log(dp->p_fd,"arp recevied source ip=%s destination ip=%s | %s %d",src_ip,dst_ip,__FILE__,__LINE__);

		char buffer[INET_ADDRSTRLEN];
		inet_ntop(AF_INET,&interface->ipv4,buffer,INET_ADDRSTRLEN);
		
		dpdk_perf_stats_log(dp->p_fd,"source interface->ipv4=%s destination ip=%s | %s %d\n",buffer,dst_ip,__FILE__,__LINE__);
		if(strcmp(buffer,dst_ip )==0)
		{
			arp_r++;
			//printf("app_info->port[%d] src MAC: %02X:%02X:%02X:%02X:%02X:%02X dst MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",interface->portid,
										//eth->src_addr.addr_bytes[0],eth->src_addr.addr_bytes[1],eth->src_addr.addr_bytes[2],
										//eth->src_addr.addr_bytes[3],eth->src_addr.addr_bytes[4],eth->src_addr.addr_bytes[5],
										//eth->dst_addr.addr_bytes[0],eth->dst_addr.addr_bytes[1],eth->dst_addr.addr_bytes[2],
										//eth->dst_addr.addr_bytes[3],eth->dst_addr.addr_bytes[4],eth->dst_addr.addr_bytes[5]);

			struct rte_ether_addr src;// = eth->src_addr;
			struct rte_ether_addr dst;
			//printf("PORT   MAC :%02X:%02X:%02X:%02X:%02X:%02X\n",interface->macaddr[0],interface->macaddr[1],
													//interface->macaddr[2],interface->macaddr[3],
													//interface->macaddr[4],interface->macaddr[5]);
			memcpy(&src,interface->macaddr,6);
			//memcpy(interface->src_macaddr,&eth->src_addr,6);
			rte_ether_addr_copy(&eth->src_addr,&eth->dst_addr);
			rte_ether_addr_copy(&src,&eth->src_addr);
			//rte_ether_addr_copy(&eth->src_addr,&src);
			//rte_ether_addr_copy(&mac_addr,&eth->src_addr);
			//(&src,&eth->dst_addr);
			//printf("source interface->ipv4=%d %s | %d\n",interface->ipv4,buffer,__LINE__);
			eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_ARP);
			//printf("-------after copy------\n");
			//printf("app_info->port[%d] src MAC: %02X:%02X:%02X:%02X:%02X:%02X dst MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",interface->portid,
										//eth->src_addr.addr_bytes[0],eth->src_addr.addr_bytes[1],eth->src_addr.addr_bytes[2],
										//eth->src_addr.addr_bytes[3],eth->src_addr.addr_bytes[4],eth->src_addr.addr_bytes[5],
										//eth->dst_addr.addr_bytes[0],eth->dst_addr.addr_bytes[1],eth->dst_addr.addr_bytes[2],
										//eth->dst_addr.addr_bytes[3],eth->dst_addr.addr_bytes[4],eth->dst_addr.addr_bytes[5]);

			
			//dpdk_perf_stats_log(dp->p_fd,"source ip=%s destination ip=%s\n",(char *)interface->ipv4,dst_ip);
			
			//memset(arp,0,sizeof(struct rte_arp_hdr ));
			arp->arp_hardware = htons(1);
			arp->arp_protocol = htons(RTE_ETHER_TYPE_IPV4);
			arp->arp_hlen     = 6;
			arp->arp_plen	  = 4;
			arp->arp_opcode   = rte_cpu_to_be_16(RTE_ARP_OP_REPLY);
			
			//arp->arp_protocol = 
			rte_ether_addr_copy(&eth->src_addr,&arp->arp_data.arp_sha);
			rte_ether_addr_copy(&eth->dst_addr,&arp->arp_data.arp_tha);
			
			struct sockaddr_in saddr;
			struct sockaddr_in daddr;

			inet_aton((char *)dst_ip,&saddr.sin_addr);
			inet_aton((char *)src_ip,&daddr.sin_addr);
			// Copy 4 bytes from the socket address into the ARP array
			memcpy(&arp->arp_data.arp_sip, &saddr.sin_addr.s_addr, sizeof(saddr.sin_addr.s_addr));
			memcpy(&arp->arp_data.arp_tip, &daddr.sin_addr.s_addr, sizeof(saddr.sin_addr.s_addr));

			char src_ip[INET_ADDRSTRLEN];
			char dst_ip[INET_ADDRSTRLEN];

			inet_ntop(AF_INET,&arp->arp_data.arp_sip,src_ip,sizeof(src_ip));
			inet_ntop(AF_INET,&arp->arp_data.arp_tip,dst_ip,sizeof(dst_ip));

			dpdk_perf_stats_log(dp->p_fd,"ARP sent source ip=%s destination ip=%s\n",src_ip,dst_ip);
			arp_s++;
			//printf(" arp packet responce arp_recvie =%d arp_sent =%d | %s %d\n",*arp_recvie,*arp_sent,__FILE__,__LINE__);
			*req_sent_code =1;
			arp_pkt_code = 3;
		} 
	}	
	*arp_recvie =arp_r;
	*arp_sent   =arp_s;
	return 0;
}

int arp_recevie(void *arg)
{
	dpdk_app_t *interface = (dpdk_app_t *)arg;
    struct rte_mbuf *pkt_mbuf[interface->brustsize];
	uint16_t iteration =0;
	//uint16_t index=0;
    uint16_t nb_rx = 0;
	uint16_t nb_rx1 = 0;
	uint16_t recvied =0;
	uint16_t pkt_recv =0;
	uint16_t lc = 1;
	
	//index = *(uint8_t *)arg;
	char buffer[200];
	//printf("DEBUG: Received a packet burst of size: %d-%d |%s|%d\n",dp->portid,interface->rx_queueid,__FILE__,__LINE__);
	dpdk_perf_stats_log(dp->p_fd,"DEBUG: Received a packet burst of size: %d-%d |%s|%d\n",interface->portid,interface->rx_queueid,__FILE__,__LINE__);
	//dpdk_perf_stats_log(dp->p_fd,buffer,59); 
	
	while(1)
	{
		//if(dp->rx == 1)
		//{
			//index=0;
		//}
		//else
		//{
			//if(iteration >= dp->rx)
				//iteration =0;
			
			//index = iteration % dp->rx;
			//iteration++;

		//}
		//for(index=0;index<dp->rx;index++)
		//{
		//nb_rx = rte_eth_rx_burst(dp->portid,dp->rx_queue[index],pkt_mbuf,BRUST_SIZE);
		nb_rx = rte_eth_rx_burst(interface->portid,interface->rx_queue[interface->rx_queueid],pkt_mbuf,interface->brustsize);
		if(nb_rx <= 0)
		{
			//printf("sleep\n");
			//sleep(10);
			usleep(10000); 
		}
		//nb_rx1 = rte_eth_rx_burst(dp->portid,1,pkt_mbuf,BRUST_SIZE);
		//if(nb_rx > 0)
		else
		{
			

			recvied =0;
			//printf("dp->portid = %d arp__rx_lc= %d  nb_rx =%d | %s %d\n",dp->portid,interface->rx_queueid,nb_rx,__FILE__,__LINE__);
			//dpdk_perf_stats_log(dp->p_fd,"dp->portid = %d arp__rx_lc= %d  nb_rx =%d | %s %d\n",dp->portid,interface->rx_queueid,nb_rx,__FILE__,__LINE__);
			
			//dpdk_perf_stats_log(dp->p_fd,"Pool=%s Total=%u Avail=%u InUse=%u\n",dp->mbuf_pool[interface->rx_queueid]->name,dp->mbuf_pool[interface->rx_queueid]->size,
      		 //rte_mempool_avail_count(dp->mbuf_pool[interface->rx_queueid]),
       		//rte_mempool_in_use_count(dp->mbuf_pool[interface->rx_queueid]));
			
			//printf("index1 = %d arp__rx_lc1= %d  nb_rx1 =%d | %s %d\n",dp->portid,lc,nb_rx1,__FILE__,__LINE__);
			
		

			for(int i=0;i<nb_rx;i++) 
			{
				dpdk_write_open_pcap(dp->dumper,pkt_mbuf[i]); 
			}
		

		//if(nb_rx > 0)
		//{

			recvied = rte_ring_enqueue_burst(interface->rxring_buffer[interface->rx_queueid],(void **)pkt_mbuf,nb_rx-recvied,NULL);
			
			//printf("arp__rx_lc= %d recvied =%d nb_rx =%d | %s %d\n",dp->rx_queue[0],recvied,nb_rx,__FILE__,__LINE__); 
			
		}
		if(recvied < nb_rx)
		{
			int rx_dropped =0;
			rx_dropped = nb_rx - recvied;
			dp->dp_stats.total_dropped += rx_dropped; 
			dpdk_perf_stats_log(dp->p_fd,"arp__rx_lc= %d recvied =%d nb_rx =%d | %s %d\n",interface->rx_queue[interface->rx_queueid],recvied,nb_rx,__FILE__,__LINE__); 
			for(int i=recvied;i < nb_rx;i++)
			{
				rte_pktmbuf_free(pkt_mbuf[i]);
			}
		}
		else{
			dp->dp_stats.total_received +=recvied; 
		}
		nb_rx = 0;
		//}
	}
	return 0;
}

int stop_gi = 0;
int stop_gtp =1;
int forward_process_packet(dpdk_app_t * interface,struct rte_mbuf **pkt_mbuf, uint16_t *recvied_i,struct rte_mbuf **process_mbuf)
{
	//struct rte_mbuf *pkt_mbuf[dp->brustsize];  
	
	uint16_t j=0;
	uint16_t i=0;
	uint16_t process =0;
	
	uint16_t arp_recvie =0;
	uint16_t arp_sent =0;
	uint16_t arp_j=0;
	
	uint16_t drop=0;
	
	uint16_t recvied = *recvied_i;
	//printf(" forward process packet  recvied =%d  | %s %d\n",recvied,__FILE__,__LINE__);
	for(i=0;i< recvied;i++)
		{
			struct rte_mbuf *m = pkt_mbuf[i];
			struct rte_ether_hdr *eth1= rte_pktmbuf_mtod(m,struct rte_ether_hdr *);
			uint16_t adj_len =0;
			//mbuf->port
			//printf("ether type = %d | %d\n",eth->ether_type,__LINE__);
			switch(eth1->ether_type)
			{
				case 8: //RTE_ETHER_TYPE_IPV4
						
				
						if(eth1->ether_type == rte_be_to_cpu_16(RTE_ETHER_TYPE_IPV4))
						{
							// if(pkt_mbuf[i]->port == 0)
							// {
								
							struct rte_ether_hdr *eth= rte_pktmbuf_mtod(m,struct rte_ether_hdr *);
							// dpdk_perf_stats_log(dp->p_fd,"GTP recevied src MAC: %02X:%02X:%02X:%02X:%02X:%02X dst MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
															// eth1->src_addr.addr_bytes[0],eth1->src_addr.addr_bytes[1],eth1->src_addr.addr_bytes[2],
															// eth1->src_addr.addr_bytes[3],eth1->src_addr.addr_bytes[4],eth1->src_addr.addr_bytes[5],
															// eth1->dst_addr.addr_bytes[0],eth1->dst_addr.addr_bytes[1],eth1->dst_addr.addr_bytes[2],
															// eth1->dst_addr.addr_bytes[3],eth1->dst_addr.addr_bytes[4],eth1->dst_addr.addr_bytes[5]);
						
							// if((eth->src_addr.addr_bytes[0] == dp->src_macaddr[0] && eth->src_addr.addr_bytes[1] == dp->src_macaddr[1] && eth->src_addr.addr_bytes[2] == dp->src_macaddr[2] &&
							// eth->src_addr.addr_bytes[3] == dp->src_macaddr[3] && eth->src_addr.addr_bytes[4] == dp->src_macaddr[4] && eth->src_addr.addr_bytes[5] == dp->src_macaddr[5])
							// && (eth->dst_addr.addr_bytes[0] == dp->macaddr[0]&& eth->dst_addr.addr_bytes[1] == dp->macaddr[1] && eth->dst_addr.addr_bytes[2] == dp->macaddr[2] &&
								// eth->dst_addr.addr_bytes[3] == dp->macaddr[3] && eth->dst_addr.addr_bytes[4] == dp->macaddr[4] && eth->dst_addr.addr_bytes[5] == dp->macaddr[5]))
							if(eth1->dst_addr.addr_bytes[0] == 0x00&& eth1->dst_addr.addr_bytes[1] == 0x0C && eth1->dst_addr.addr_bytes[2] == 0x29 &&
								eth1->dst_addr.addr_bytes[3] == 0x25 && eth1->dst_addr.addr_bytes[4] == 0x4F && eth1->dst_addr.addr_bytes[5] == 0xD8)			
							{
								
								uint32_t verfiy_len = m->pkt_len;
								dpdk_perf_stats_log(dp->p_fd,"1.GTP after recevied m->pkt_len=%d data=%d, off=%d|%s|%d",m->pkt_len,m->data_len,m->data_off,__FILE__,__LINE__);
								//printf("GTP before recevied m->pkt_len=%d\n",m->pkt_len);
								
								struct rte_ipv4_hdr *ipv4 = (struct rte_ipv4_hdr *)(eth1+1);
								struct rte_udp_hdr *udp = (struct rte_udp_hdr *)(ipv4+1);
								struct rte_gtp_hdr *gtp = (struct rte_gtp_hdr *)(udp+1);
								if(ipv4->total_length  > 30)
								{
									
									if(udp->dgram_len  > 40)
									{
										
										if(gtp->plen  > 48)
										{
											adj_len =sizeof(struct rte_ether_hdr)+ sizeof(struct rte_ipv4_hdr)+ sizeof(struct rte_udp_hdr) + sizeof(struct rte_gtp_hdr);
											//rte_pktmbuf_adj( m,(uint16_t)(sizeof(struct rte_ipv4_hdr)));
											//rte_pktmbuf_adj( m,(uint16_t)(sizeof(struct rte_udp_hdr)));									
											//rte_pktmbuf_adj( m,(uint16_t)(sizeof(struct rte_gtp_hdr)));	
										}
									}
								}
								
								
								
								//struct rte_ether_hdr *eth=(struct rte_ether_hdr *) rte_pktmbuf_prepend(m,(uint16_t)sizeof(struct rte_ether_hdr));
								
	
								
								/*
								if(ipv4->time_to_live <= 1)
								{
									break;
								}
								else
								{
									ipv4->time_to_live--;
								}
								//ipv4->total_length = rte_cpu_to_be_16(m->pkt_len -(sizeof(struct rte_ether_hdr)+sizeof(struct rte_ipv4_hdr)+ sizeof(struct rte_udp_hdr)+sizeof(struct rte_gtp_hdr)));
								ipv4->total_length = rte_cpu_to_be_16((m->pkt_len)-14);
								uint32_t cpy = ipv4->src_addr;
								ipv4->src_addr = ipv4->dst_addr;
								ipv4->dst_addr = cpy;
								
								uint32_t src_addr = ipv4->src_addr;
								uint32_t dst_addr = ipv4->dst_addr;
								uint32_t src = rte_be_to_cpu_32(src_addr);
								uint32_t dst = rte_be_to_cpu_32(dst_addr); 
								
								dpdk_perf_stats_log(dp->p_fd,"GTP recevied src ip : %d.%d.%d.%d\n",(src >> 24)& 0xFF,(src >> 16)&0xFF,(src >> 8)& 0xFF,(src & 0xFF));
								dpdk_perf_stats_log(dp->p_fd,"GTP recevied dst ip : %d.%d.%d.%d\n",(dst >> 24)& 0xFF,(dst >> 16)&0xFF,(dst >> 8)& 0xFF,(dst & 0xFF));
								ipv4->hdr_checksum = 0;
								ipv4->hdr_checksum = rte_ipv4_cksum(ipv4);
								
								dpdk_perf_stats_log(dp->p_fd,"2.GTP after recevied m->pkt_len=%d len=%u",m->pkt_len,rte_be_to_cpu_16(ipv4->total_length));
								
								struct rte_tcp_hdr *tcp = (struct rte_tcp_hdr *)(ipv4 + 1);
								struct rte_udp_hdr *udp = (struct rte_udp_hdr *)(ipv4+1);
								
								if(ipv4->next_proto_id == 6) 		//6  = TCP   17 = UDP   1  = ICMP
								{

									

									uint16_t s_port = tcp->src_port;
									tcp->src_port = tcp->dst_port;
									tcp->dst_port = s_port;
									//tcp->sent_seq =  
									//tcp->
									//printf("src port %d:dst port  %d\n",tcp->src_port,tcp->dst_port);
									//dpdk_perf_stats_log(dp->p_fd,"src port %d:dst port  %d\n",tcp->src_port,tcp->dst_port);
									tcp->cksum = 0;
									tcp->cksum = rte_ipv4_udptcp_cksum(ipv4,tcp);
								}
								else if(ipv4->next_proto_id == 17) 
								{
									

									uint16_t u_port =udp->src_port;
									udp->src_port = udp->dst_port;
									udp->dst_port = u_port;
									
									udp->dgram_len = rte_cpu_to_be_16((m->pkt_len) -14-20); 
									
									udp->dgram_cksum = 0;
									//udp->dgram_cksum = rte_ipv4_udptcp_cksum(ipv4 , udp);
									//dpdk_perf_stats_log(dp->p_fd,"GTP after len=%u dgram=%u",((m->pkt_len) -14-20),rte_be_to_cpu_16(udp->dgram_len));
								
								}
								struct rte_gtp_hdr *gtp = (struct rte_gtp_hdr *)(udp+1);
								
								udp->dgram_cksum = rte_ipv4_udptcp_cksum(ipv4 , udp);
								*/
								if(adj_len >= 48 && adj_len  <= 54)
								{
									rte_pktmbuf_adj( m,adj_len);
								}
								struct rte_ipv4_hdr *i_ipv4 = rte_pktmbuf_mtod(m,struct rte_ipv4_hdr *);
								struct rte_udp_hdr *i_udp = (struct rte_udp_hdr *)(i_ipv4+1);
								
							
															
								dpdk_perf_stats_log(dp->p_fd,"2.GTP after recevied m->pkt_len=%d data=%d, adj_len=%d|%s|%d",m->pkt_len,m->data_len,adj_len,__FILE__,__LINE__);
								
								//i_ipv4->version=4;
								//i_ipv4->ihl =5;
								//i_ipv4->total_length = rte_cpu_to_be_16((m->pkt_len)-14);
								uint32_t ipv4_s_cpy = i_ipv4->src_addr;
								uint32_t ipv4_d_cpy = i_ipv4->dst_addr;
								//i_ipv4->src_addr = i_ipv4->dst_addr;
								//i_ipv4->dst_addr = ipv4_s_cpy;
								
								//i_ipv4->hdr_checksum = 0;
								//i_ipv4->hdr_checksum = rte_ipv4_cksum(i_ipv4);
								
								
								
								uint16_t s_cpy =i_udp->src_port;
								uint16_t d_cpy =i_udp->dst_port;
								
								//i_udp->src_port = i_udp->dst_port;
								//i_udp->dst_port = s_cpy;
									
								//i_udp->dgram_len = rte_cpu_to_be_16((m->pkt_len) -14-20); 
									
								//i_udp->dgram_cksum = 0;
								//i_udp->dgram_cksum = rte_ipv4_udptcp_cksum(i_ipv4 , i_udp);
								
								
								
								eth1= (struct rte_ether_hdr *)rte_pktmbuf_prepend(m,14);
								if(eth1 == NULL)
								{
									break;
								}
								eth1->ether_type = 8;//rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
								
								uint8_t raw[6] = {0x00,0x0C,0x29,0x25,0x4F,0xE2}; //00:0C:29:25:4F:E2
								uint8_t raw_dst[6] = {0x00,0x0C,0x29,0x30,0x47,0xDD}; //00:0C:29:30:47:DD
								// uint8_t raw[8] = {0x00,0x0C,0x29,0x25,0x4F,0xD8};
									//pci bus 1
								eth1->src_addr.addr_bytes[0]=interface->Next->macaddr[0];
								eth1->src_addr.addr_bytes[1]=interface->Next->macaddr[1];
								eth1->src_addr.addr_bytes[2]=interface->Next->macaddr[2];
								eth1->src_addr.addr_bytes[3]=interface->Next->macaddr[3];
								eth1->src_addr.addr_bytes[4]=interface->Next->macaddr[4];
								eth1->src_addr.addr_bytes[5]=interface->Next->macaddr[5];
								
									////pci bus 2
								// eth1->src_addr.addr_bytes[0]=raw[0];
								// eth1->src_addr.addr_bytes[1]=raw[1];
								// eth1->src_addr.addr_bytes[2]=raw[2];
								// eth1->src_addr.addr_bytes[3]=raw[3];
								// eth1->src_addr.addr_bytes[4]=raw[4];
								// eth1->src_addr.addr_bytes[5]=raw[5];

								eth1->dst_addr.addr_bytes[0]=raw_dst[0];
								eth1->dst_addr.addr_bytes[1]=raw_dst[1];
								eth1->dst_addr.addr_bytes[2]=raw_dst[2];
								eth1->dst_addr.addr_bytes[3]=raw_dst[3];
								eth1->dst_addr.addr_bytes[4]=raw_dst[4];
								eth1->dst_addr.addr_bytes[5]=raw_dst[5];
								
								
								i_ipv4->version=4;
								i_ipv4->ihl =5;
								i_ipv4->total_length = rte_cpu_to_be_16((m->pkt_len)-14);
								i_ipv4->src_addr = ipv4_s_cpy;
								i_ipv4->dst_addr = ipv4_d_cpy;
								i_ipv4->hdr_checksum = 0;
								i_ipv4->hdr_checksum = rte_ipv4_cksum(i_ipv4);
								
								uint32_t src_addr = i_ipv4->src_addr;
								uint32_t dst_addr = i_ipv4->dst_addr;
								uint32_t src = rte_be_to_cpu_32(src_addr);
								uint32_t dst = rte_be_to_cpu_32(dst_addr); 
								
								dpdk_perf_stats_log(dp->p_fd,"GTP recevied src ip : %d.%d.%d.%d|%s |%d",(src >> 24)& 0xFF,(src >> 16)&0xFF,(src >> 8)& 0xFF,(src & 0xFF),__FILE__,__LINE__);
								dpdk_perf_stats_log(dp->p_fd,"GTP recevied dst ip : %d.%d.%d.%d|%s |%d",(dst >> 24)& 0xFF,(dst >> 16)&0xFF,(dst >> 8)& 0xFF,(dst & 0xFF),__FILE__,__LINE__);
								
								//s_cpy =i_udp->src_port;
								//d_cpy =i_udp->dst_port;
								
								i_udp->src_port = s_cpy;
								i_udp->dst_port = d_cpy;
									
								i_udp->dgram_len = rte_cpu_to_be_16((m->pkt_len) -14-20); 
									
								i_udp->dgram_cksum = 0;
								i_udp->dgram_cksum = rte_ipv4_udptcp_cksum(i_ipv4 , i_udp);
								//struct rte_ether_addr addr;// = eth->src_addr;
								//rte_ether_addr_copy(&eth1->dst_addr,&addr);
								//rte_ether_addr_copy(&eth1->src_addr,&eth1->dst_addr);
								//rte_ether_addr_copy(&addr,&eth1->src_addr); 
								
								dpdk_perf_stats_log(dp->p_fd,"2.1GTP after recevied m->pkt_len=%d data=%d, adj_len=%d|%s |%d",m->pkt_len,m->data_len,adj_len,__FILE__,__LINE__);
								
								dpdk_perf_stats_log(dp->p_fd,"GTP recevied src MAC: %02X:%02X:%02X:%02X:%02X:%02X dst MAC: %02X:%02X:%02X:%02X:%02X:%02X|%s |%d",
															eth1->src_addr.addr_bytes[0],eth1->src_addr.addr_bytes[1],eth1->src_addr.addr_bytes[2],
															eth1->src_addr.addr_bytes[3],eth1->src_addr.addr_bytes[4],eth1->src_addr.addr_bytes[5],
															eth1->dst_addr.addr_bytes[0],eth1->dst_addr.addr_bytes[1],eth1->dst_addr.addr_bytes[2],
															eth1->dst_addr.addr_bytes[3],eth1->dst_addr.addr_bytes[4],eth1->dst_addr.addr_bytes[5],__FILE__,__LINE__);
								
								dpdk_perf_stats_log(dp->p_fd,"src port %d:dst port  %d|%s |%d",rte_be_to_cpu_16(i_udp->src_port),rte_be_to_cpu_16(i_udp->dst_port),__FILE__,__LINE__);
								
								//dpdk_perf_stats_log(dp->p_fd,"3.GTP after recevied m->pkt_len=%d data=%d, off=%d",m->pkt_len,m->data_len,m->data_off);
								if(m->pkt_len == 142 && verfiy_len == 178)
								{
									
									gtp_pkt_code = 1;
									dpdk_perf_stats_log(dp->p_fd,"3.GI after recevied m->pkt_len=%d data=%d, off=%d |%s |%d",m->pkt_len,m->data_len,gtp_pkt_code,__FILE__,__LINE__);
									
									
								}
								process_mbuf[process];
								process++;
								
								//dpdk_write_open_pcap(dp->dumper,m);
								//exit(1); 
							
							}
							//}
							//else if(pkt_mbuf[i]->port == 1)//00:0C:29:25:4F:E2
							else if(eth1->dst_addr.addr_bytes[0] == 0x00&& eth1->dst_addr.addr_bytes[1] == 0x0C && eth1->dst_addr.addr_bytes[2] == 0x29 &&
								eth1->dst_addr.addr_bytes[3] == 0x25 && eth1->dst_addr.addr_bytes[4] == 0x4F && eth1->dst_addr.addr_bytes[5] == 0xE2)
									
							{
								
								dpdk_perf_stats_log(dp->p_fd,"1.GI after recevied m->pkt_len=%d data=%d, off=%d|%s |%d",m->pkt_len,m->data_len,m->data_off,__FILE__,__LINE__);
								
								uint32_t check_len = m->pkt_len;
   								rte_pktmbuf_adj( m,14);
								struct rte_ipv4_hdr *i_ipv4 = rte_pktmbuf_mtod(m,struct rte_ipv4_hdr *);//(struct rte_ipv4_hdr *)(eth1+1); 
								struct rte_udp_hdr *i_udp = (struct rte_udp_hdr *)(i_ipv4+1);
								
								uint32_t ipv4_s_cpy = i_ipv4->src_addr;
								uint32_t ipv4_d_cpy = i_ipv4->dst_addr;
								
								uint16_t s_cpy =i_udp->src_port;
								uint16_t d_cpy =i_udp->dst_port;
								
								dpdk_perf_stats_log(dp->p_fd,"GI recevied src ip : %d.%d.%d.%d|%s |%d",(ipv4_s_cpy >> 24)& 0xFF,(ipv4_s_cpy >> 16)&0xFF,(ipv4_s_cpy >> 8)& 0xFF,(ipv4_s_cpy & 0xFF),__FILE__,__LINE__);
								dpdk_perf_stats_log(dp->p_fd,"GI recevied dst ip : %d.%d.%d.%d|%s |%d",(ipv4_d_cpy >> 24)& 0xFF,(ipv4_d_cpy >> 16)&0xFF,(ipv4_d_cpy >> 8)& 0xFF,(ipv4_d_cpy & 0xFF),__FILE__,__LINE__);
								dpdk_perf_stats_log(dp->p_fd,"src port %d:dst port  %d|%s |%d",rte_be_to_cpu_16(i_udp->src_port),rte_be_to_cpu_16(i_udp->dst_port),__FILE__,__LINE__);
								
								dpdk_perf_stats_log(dp->p_fd,"before");
								
								
								
								//rte_pktmbuf_adj( m,14); // cmt on 12/08/26
								
								eth1= (struct rte_ether_hdr *)rte_pktmbuf_prepend(m,50);
								
								if(eth1 == NULL)
								{
									dpdk_perf_stats_log(dp->p_fd,"NULL prepend is Null |%s |%d",__FILE__,__LINE__);
								
									break;
								}
								
								dpdk_perf_stats_log(dp->p_fd,"1.1 GI after recevied m->pkt_len=%d data=%d, off=%d|%s |%d",m->pkt_len,m->data_len,m->data_off,__FILE__,__LINE__);
								
								struct rte_ipv4_hdr *o_ipv4 =(struct rte_ipv4_hdr *)(eth1+1);// rte_pktmbuf_mtod(m,struct rte_ipv4_hdr *);
								
								struct rte_udp_hdr *o_udp = (struct rte_udp_hdr *)(o_ipv4+1);//rte_pktmbuf_mtod(m,struct rte_udp_hdr *);
								struct rte_gtp_hdr *gtp = (struct rte_gtp_hdr *)(o_udp+1);//rte_pktmbuf_mtod(m,struct rte_gtp_hdr *);
								
								
								uint8_t raw[6] = {0x00,0x0C,0x29,0x25,0x4F,0xD8}; //00:0C:29:25:4F:D8
								uint8_t raw_dst[6] = {0x00,0x0C,0x29,0x30,0x47,0xD3}; //00:0C:29:30:47:D3
								// uint8_t raw[8] = {0x00,0x0C,0x29,0x25,0x4F,0xD8};
									//pci bus 1
								// eth1->src_addr.addr_bytes[0]=dp->macaddr[0];
								// eth1->src_addr.addr_bytes[1]=dp->macaddr[1];
								// eth1->src_addr.addr_bytes[2]=dp->macaddr[2];
								// eth1->src_addr.addr_bytes[3]=dp->macaddr[3];
								// eth1->src_addr.addr_bytes[4]=dp->macaddr[4];
								// eth1->src_addr.addr_bytes[5]=dp->macaddr[5];
								
								
					
								
								
								o_ipv4->version=0;
								o_ipv4->src_addr=0;
								o_ipv4->dst_addr=0;

								o_ipv4->version=4;
								o_ipv4->ihl =5;
								//o_ipv4->total_length =rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr));
									uint16_t gtp_pkt_len = 14+20+8+8+20+8+100; 
								o_ipv4->packet_id = gtp_pkt_len;
								//o_ipv4->flags =0;
								//o_ipv4->fragment_offset.flags = 0;
								o_ipv4->fragment_offset =rte_cpu_to_be_16(0x4000);//(RTE_IPV4_HDR_DF_FLAG);
								o_ipv4->time_to_live=64;
								o_ipv4->next_proto_id = 17;
								//uint32_t src_addr=3232272397; //192.168.144.13
								o_ipv4->src_addr= rte_cpu_to_be_32(3232272437);
								o_ipv4->dst_addr=rte_cpu_to_be_32(3232272405);
								o_ipv4->total_length =rte_cpu_to_be_16((m->pkt_len)-14);
								o_ipv4->hdr_checksum = 0;
								o_ipv4->hdr_checksum = rte_ipv4_cksum(o_ipv4);
								
								
								
								o_udp->src_port =rte_cpu_to_be_16(2152);
								o_udp->dst_port = rte_cpu_to_be_16(2152);
								
								
								o_udp->dgram_len = rte_cpu_to_be_16((m->pkt_len)-14-20);
								o_udp->dgram_cksum =0;
								
								
								//GTP-U layer
								
								//struct rte_gtp_hdr *gtp = rte_pktmbuf_mtod_offset(create_pkt,struct rte_gtp_hdr*,sizeof(struct rte_ipv4_hdr)+sizeof(struct rte_ether_hdr)+sizeof(struct rte_udp_hdr));
								
								gtp->gtp_hdr_info =48;
								gtp->msg_type = 255;
								gtp->plen =rte_cpu_to_be_16((m->pkt_len)-14-20-8);
								gtp->teid = rte_cpu_to_be_32(0x1);
								
								o_udp->dgram_cksum = rte_ipv4_udptcp_cksum(o_ipv4 , o_udp);
								
								
								i_ipv4->version=4;
								i_ipv4->ihl =5;
								i_ipv4->total_length = rte_cpu_to_be_16((m->pkt_len)-14-20-8-8);
								i_ipv4->src_addr =ipv4_s_cpy;
								i_ipv4->dst_addr = ipv4_d_cpy;
								i_ipv4->hdr_checksum = 0;
								i_ipv4->hdr_checksum = rte_ipv4_cksum(i_ipv4);
								
								uint32_t src_addr = i_ipv4->src_addr;
								uint32_t dst_addr = i_ipv4->dst_addr;
								uint32_t src = rte_be_to_cpu_32(src_addr);
								uint32_t dst = rte_be_to_cpu_32(dst_addr); 
								
								dpdk_perf_stats_log(dp->p_fd,"GI recevied src ip : %d.%d.%d.%d|%s |%d",(src >> 24)& 0xFF,(src >> 16)&0xFF,(src >> 8)& 0xFF,(src & 0xFF),__FILE__,__LINE__);
								dpdk_perf_stats_log(dp->p_fd,"GI recevied dst ip : %d.%d.%d.%d|%s |%d",(dst >> 24)& 0xFF,(dst >> 16)&0xFF,(dst >> 8)& 0xFF,(dst & 0xFF),__FILE__,__LINE__);
								
								
								
								i_udp->src_port = s_cpy;
								i_udp->dst_port = d_cpy; 
									
								i_udp->dgram_len = rte_cpu_to_be_16((m->pkt_len)-14-20-8-8-20); 
									
								i_udp->dgram_cksum = 0;
								i_udp->dgram_cksum = rte_ipv4_udptcp_cksum(i_ipv4 , i_udp);
								
								
								
								eth1->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
								
								// eth1->src_addr.addr_bytes[0]=dp->macaddr[0];
								// eth1->src_addr.addr_bytes[1]=dp->macaddr[1];
								// eth1->src_addr.addr_bytes[2]=dp->macaddr[2];
								// eth1->src_addr.addr_bytes[3]=dp->macaddr[3];
								// eth1->src_addr.addr_bytes[4]=dp->macaddr[4];
								// eth1->src_addr.addr_bytes[5]=dp->macaddr[5];
								
								eth1->src_addr.addr_bytes[0]=raw[0];
								eth1->src_addr.addr_bytes[1]=raw[1];
								eth1->src_addr.addr_bytes[2]=raw[2];
								eth1->src_addr.addr_bytes[3]=raw[3];
								eth1->src_addr.addr_bytes[4]=raw[4];
								eth1->src_addr.addr_bytes[5]=raw[5];

								eth1->dst_addr.addr_bytes[0]=raw_dst[0];
								eth1->dst_addr.addr_bytes[1]=raw_dst[1];
								eth1->dst_addr.addr_bytes[2]=raw_dst[2];
								eth1->dst_addr.addr_bytes[3]=raw_dst[3];
								eth1->dst_addr.addr_bytes[4]=raw_dst[4];
								eth1->dst_addr.addr_bytes[5]=raw_dst[5];
								
								
								
								dpdk_perf_stats_log(dp->p_fd,"2.1 GI after recevied m->pkt_len=%d data=%d, adj_len=%d|%s |%d",m->pkt_len,m->data_len,adj_len,__FILE__,__LINE__);
								
								dpdk_perf_stats_log(dp->p_fd,"GI recevied src MAC: %02X:%02X:%02X:%02X:%02X:%02X dst MAC: %02X:%02X:%02X:%02X:%02X:%02X |%s |%d",
															eth1->src_addr.addr_bytes[0],eth1->src_addr.addr_bytes[1],eth1->src_addr.addr_bytes[2],
															eth1->src_addr.addr_bytes[3],eth1->src_addr.addr_bytes[4],eth1->src_addr.addr_bytes[5],
															eth1->dst_addr.addr_bytes[0],eth1->dst_addr.addr_bytes[1],eth1->dst_addr.addr_bytes[2],
															eth1->dst_addr.addr_bytes[3],eth1->dst_addr.addr_bytes[4],eth1->dst_addr.addr_bytes[5],__FILE__,__LINE__);
								
								dpdk_perf_stats_log(dp->p_fd,"src port %d:dst port  %d |%s |%d",rte_be_to_cpu_16(i_udp->src_port),rte_be_to_cpu_16(i_udp->dst_port),__FILE__,__LINE__);
								//uint8_t del_len = sizeof(struct rte_ipv4_hdr)+ sizeof(struct rte_udp_hdr) + sizeof(struct rte_gtp_hdr);
								//m->pkt_len -= del_len; 
								
								if(m->pkt_len == 178 && check_len == 142)
								{
									// uint16_t temp=0;
									// temp = rte_eth_tx_burst(0,0,&m,1);
									// dpdk_perf_stats_log(dp->p_fd,"create gtp packet transmit =%d  | %s %d\n",temp,__FILE__,__LINE__); 
									// for(int i=0;i<temp;i++) 
									// {
										// dpdk_write_open_pcap(dp->dumper,m); 
									// }
									gi_pkt_code = 2;
									dpdk_perf_stats_log(dp->p_fd,"3.GI after recevied m->pkt_len=%d data=%d, off=%d |%s |%d",m->pkt_len,m->data_len,gi_pkt_code,__FILE__,__LINE__);
									
									
								}
								process_mbuf[process];
								process++;
								
							} 
						}
						//rte_eth_tx_burst(0,0,pkt_mbuf,1);
						break;
				case 1544 : //RTE_ETHER_TYPE_ARP	
							//dpdk_app_t * interface = dp;
							//char buffer[INET_ADDRSTRLEN];
							//inet_ntop(AF_INET,&interface->ipv4,buffer,INET_ADDRSTRLEN);	
							//dpdk_perf_stats_log(dp->p_fd,"source interface->ipv4=%u destination ip=%u |%s|%d",interface->ipv4,interface->Destip,__FILE__,__LINE__);
							
							//arp_recvie =0;
							//arp_sent =0;
							int req_sent_code =0;
							 arp_packet_responce(interface,m,&arp_recvie,&arp_sent,&req_sent_code);
							 if(arp_recvie > 0)
							 {
							// printf(" arp_packet_responce arp_recvie =%d arp_sent =%d | %s %d\n",arp_recvie,arp_sent,__FILE__,__LINE__);
							 dp->dp_stats.arp_request_received +=arp_recvie;  
							 dp->dp_stats.arp_responce_sent += arp_sent;
							 }
							 //printf(" arp_packet_responce req_sent_code =%d | %s %d\n",req_sent_code,__FILE__,__LINE__);
							 if(req_sent_code == 1)
							 {
								//arp_send_request(dp);
								req_sent_code = 0;
							 }
							 process_mbuf[process];
							process++;
							// arp_j +=1;
							break;

				default :   rte_pktmbuf_free(m);
							drop += 1;
							dp->dp_stats.la_p_dropped += drop;
							break;
			}
			
		}
	j =process ;//+ arp_j;
	//process_mbuf[j];
	return j;

}
int arp_worker(void *arg)
{
	dpdk_app_t *interface = (dpdk_app_t *)arg;
    struct rte_mbuf *pkt_mbuf[interface->brustsize]; 
    struct rte_mbuf *process_mbuf[interface->brustsize]; 
    struct rte_mbuf *drop_mbuf[interface->brustsize]; 
    uint16_t recvied =0;
	uint16_t worker =0;
	uint16_t process =0;
	uint16_t arp_recvie =0;
	uint16_t arp_sent =0;
	uint16_t index =0;
	uint16_t iteration =0;
	
	uint16_t process_pkts =0;
	int tx_index =0;
	int tx_iteration =0;
	
	uint16_t nb_tx=0;
	
    int i=0;
	uint16_t drop=0;
	uint16_t adj_len =0;
	//index = *(uint8_t *)arg;
	printf("DEBUG: launched a arp worker: %d-%d |%s|%d\n",interface->portid,interface->worker_queue[interface->workerid],__FILE__,__LINE__);
	dpdk_perf_stats_log(dp->p_fd,"DEBUG: launched a arp worker: %d-%d |%s|%d\n",interface->portid,interface->worker_queue[interface->workerid],__FILE__,__LINE__);;
	struct rte_ether_addr mac_addr;
	int ret = rte_eth_macaddr_get(interface->portid,&mac_addr);
	if(ret < 0)
	{
		rte_exit(EXIT_FAILURE,"Failed to get mac_addr| %d\n",__LINE__);
	}
	//printf("MAC :%02X:%02X:%02X:%02X:%02X:%02X\n",mac_addr.addr_bytes[0],mac_addr.addr_bytes[1],
												//mac_addr.addr_bytes[2],mac_addr.addr_bytes[3],
												//mac_addr.addr_bytes[4],mac_addr.addr_bytes[5]);
	while(1)
	{
		if(interface->rx == 1)
		{
			index=0;
		}
		else
		{
			if(iteration >= interface->rx)
				iteration =0;
			
			index = iteration % interface->rx;
			iteration++;

		}
		recvied = rte_ring_dequeue_burst(interface->rxring_buffer[index],(void **)pkt_mbuf,interface->brustsize,NULL);
		dp->dp_stats.total_dequeued +=recvied;
		//worker =0;
		if (recvied <= 0)
		{
			usleep(10000); 
			//printf("rte_ring_dequeue_burst faild=%d | %d\n",recvied,__LINE__);
			//rte_exit(EXIT_FAILURE,"Failed to rte_ring_dequeue_burst| %d\n",__LINE__);
       		//return -1;
		}else
		{
			//printf(" arp_work_lc= %d recvied =%d  | %s %d\n",dp->worker_queue[interface->workerid],recvied,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd,"RX worker logs buffer=%p  |%s|%d",interface->rxring_buffer[index],__FILE__,__LINE__); 
				
		}
		
		if(recvied > 0)
		{
			process_pkts = forward_process_packet(interface,pkt_mbuf,&recvied,process_mbuf);
		}
		
		
		if(interface->tx == 1)
		{
			tx_index=0;
		}
		else
		{
			if(tx_iteration >= interface->tx)
				tx_iteration =0;
			
			tx_index = tx_iteration % interface->tx; 
			tx_iteration++;

		}
		
		//interface 
		
		worker = rte_ring_enqueue_burst(interface->tx_interface->txring_buffer[tx_index],(void **)pkt_mbuf,recvied,NULL);
		if(recvied > 0)
		{
			dpdk_perf_stats_log(dp->p_fd,"worker send logs  recvied=%d process_pkts=%d worker=%d |%s|%d",recvied,process_pkts,worker,__FILE__,__LINE__);
			//dpdk_perf_stats_log(dp->p_fd,"worker send logs  buffer=%p |%s|%d",interface->tx_interface->txring_buffer[tx_index],__FILE__,__LINE__);
			//printf("arp_work_lc= %d recvied =%d worker =%d | %s %d\n",tx_index,recvied,worker,__FILE__,__LINE__);  
		
		dp->dp_stats.total_enqueued  += worker;
		//dp->dp_stats.total_proceed   += process;
		dp->dp_stats.total_proceed   += process_pkts;
		}
		
		if(worker < recvied )
		{
			dpdk_perf_stats_log(dp->p_fd,"worker dropped packets worker=%d recvied=%d |%s|%d",worker,recvied,__FILE__,__LINE__);
			for(int i=worker;i < recvied;i++)
			{
				rte_pktmbuf_free(pkt_mbuf[i]);
			}
		}
		
		// for(int i=0;i<recvied;i++) 
		// {
			////printf(" arp send recvied =%d nb_rx =%d | %s %d\n",recvied,nb_tx,__FILE__,__LINE__);
			// dpdk_write_open_pcap(dp->dumper,pkt_mbuf[i]);
			// dpdk_perf_stats_log(dp->p_fd,"send logs pkt_len=%d port=%d buffer=%p |%s|%d",pkt_mbuf[i]->pkt_len,pkt_mbuf[i]->port,interface->txring_buffer[interface->tx_queueid],__FILE__,__LINE__); 
			////exit(1);
		// }
	
		// nb_tx = rte_eth_tx_burst(0,0,pkt_mbuf,recvied - nb_tx );
		// dpdk_perf_stats_log(dp->p_fd,"send logs buffer=%p |%s|%d",interface->txring_buffer[interface->tx_queueid],__FILE__,__LINE__); 
		// nb_tx=0;
		recvied = 0;
		worker =0;
		process = 0;
		arp_recvie=0;
		arp_sent =0;
		drop =0;
		process_pkts =0;
    }
	return 0;
}

int arp_send(void *arg)
{
	dpdk_app_t *interface = (dpdk_app_t *)arg;
    struct rte_mbuf *pkt_mbuf[interface->brustsize];
    uint16_t nb_tx;
    uint16_t recvied =0;
    
	uint16_t index =0;
	uint16_t iteration =0;
	
	
	printf("DEBUG: launched a arp send: %d-%d |%s|%d\n",interface->portid,interface->tx_queue[interface->tx_queueid],__FILE__,__LINE__);
	dpdk_perf_stats_log(dp->p_fd,"DEBUG: launched a arpsend: %d-%d buffer=%p |%s|%d\n",interface->portid,interface->tx_queue[interface->tx_queueid],
	interface->txring_buffer[interface->tx_queueid],__FILE__,__LINE__);
	//printf("DEBUG: Received a packet burst of size: %d\n",dp->portid);
	while(1)
	{
		// if(interface->tx == 1)
		// {
			// index = 0;
		// }
		// else
		// {
			// if(iteration >= interface->tx)
				// iteration =0;
			
			// index = iteration % interface->tx;
			// iteration++;

		// }
		recvied = rte_ring_dequeue_burst(interface->txring_buffer[interface->tx_queueid],(void **)pkt_mbuf,interface->brustsize,NULL);
		nb_tx = 0;
		if(recvied <= 0)
		{
			usleep(10000); 
			//printf("rte_ring_dequeue_burst faild=%d | %d\n",recvied,__LINE__);
			//rte_exit(EXIT_FAILURE,"Failed to rte_ring_dequeue_burst| %d\n",__LINE__);
       		//return -1;
		}
		else
		{
			//printf("index =%d arp send_lc= %d recvied =%d  | %s %d\n",index,dp->tx_queue[index],recvied,__FILE__,__LINE__);
		}

		if(recvied > 0)
		{

			for(int i=0;i<recvied;i++) 
			{
				//printf(" arp send recvied =%d nb_rx =%d | %s %d\n",recvied,nb_tx,__FILE__,__LINE__);
				dpdk_write_open_pcap(dp->dumper,pkt_mbuf[i]);
				//dpdk_perf_stats_log(dp->p_fd,"send logs pkt_len=%d port=%d buffer=%p |%s|%d",pkt_mbuf[i]->pkt_len,pkt_mbuf[i]->port,interface->txring_buffer[interface->tx_queueid],__FILE__,__LINE__); 
				
				//exit(1);
			}
			
			 
			
				nb_tx = rte_eth_tx_burst(interface->portid,interface->tx_queue[interface->tx_queueid],pkt_mbuf,recvied);
				if(gtp_pkt_code == 1 || arp_pkt_code == 3 || gi_pkt_code == 2 )
				 {
				dpdk_perf_stats_log(dp->p_fd,"send logs port=%d buffer=%p arp=%d gtp=%d gi=%d |%s|%d",interface->portid,interface->txring_buffer[interface->tx_queueid],arp_pkt_code,gtp_pkt_code,gi_pkt_code,__FILE__,__LINE__); 
				gtp_pkt_code =0;
				gi_pkt_code =0;
				arp_pkt_code =0;
				}
			
			
			
		}
			if(nb_tx < recvied )
			{
				dp->dp_stats.la_tx_dropped += recvied - nb_tx; 
				dpdk_perf_stats_log(dp->p_fd,"arp recvied=%d transmit =%d  | %s %d\n",recvied,nb_tx,__FILE__,__LINE__); 
				for(int i=nb_tx;i < recvied;i++)
				{
					rte_pktmbuf_free(pkt_mbuf[i]);
				}
			}
			else
			{
				dp->dp_stats.total_sent +=nb_tx;
			}
			recvied =0 ;
	}
	return 0;
}

int dpdk_config__get_int( json_t * json_config, char * key, int defval, int minval)
{
	json_t * jObj = json_object_get( json_config, key);
	if( jObj) {
		int v = json_integer_value( jObj);
		if( v < minval)
			return minval;
		else
			return v;
	}
	return defval;
}
int dpdk_config_integer_value(json_t *interface,char *key,int maxlen)
{
	json_t *obj = json_object_get(interface,key);
	if(obj)
	{
		int kv = json_integer_value(obj);
		//size_t len = json_integer_length(obj);
		return kv;
	}
	printf("BUS failed address: %s |%s| %d\n",key,__FILE__,__LINE__);
	return maxlen;

}

int dpdk_config_string_value(json_t *interface,char *key,char *value,int maxlen)
{
	json_t *obj = json_object_get(interface,key);
	if(obj)
	{
		const char *kv = json_string_value(obj);
		size_t len = json_string_length(obj);
		if(len < maxlen)
		{
			memcpy(value,kv,len);
			//printf("BUS id address: %s |%s| %d\n",value,__FILE__,__LINE__);
		}
		else
		{
			memcpy(value,kv,maxlen);
			//printf("BUS id address: %s |%s| %d\n",value,__FILE__,__LINE__);
		}
	}
	else
	{
		printf("BUS failed address: %s |%s| %d\n",key,__FILE__,__LINE__);
		return -1;
	}
	return 1;
}


void dpe_configure(char *file)
{
	dpdk_app_t *current=NULL;
	json_t *config;
	json_error_t error;
	memset(&error,0,sizeof(json_error_t));

	config = json_load_file(file,0,&error);
	if(!config)
	{
		printf("json not load file col=%d line=%d pos=%d sourc=%s text=%s  %s| %d\n",error.column,error.line,error.position,error.source,error.text,__FILE__,__LINE__);;
		exit(EXIT_FAILURE);
	} 
	else
	{
		printf("loaded json file =success %s|%d\n",__FILE__,__LINE__);
		json_t * interface = json_object_get(config,"interface");
		if(!interface)
		{
			printf("please configure interface %s |%d\n",__FILE__,__LINE__);
			exit(0);
		}
		
			printf("configured interface %p %s|%d\n",interface,__FILE__,__LINE__);

			int count = json_array_size(interface);
			printf("total array in interface =%d %s | %d\n",count,__FILE__,__LINE__);
			for(int i=0;i<count ;i++)
			{
				current = (dpdk_app_t *)malloc(sizeof(dpdk_app_t));
				if(current == NULL)
				{
					printf("memory allocation failed %s | %d\n",__FILE__,__LINE__);
					exit(-1);
				}
				printf("------- Interface %d -----------\n",i); 
				json_t *index = json_array_get(interface,i);
				//printf("index: %p |%s| %d\n",index,__FILE__,__LINE__);
				//int ret = dpdk_config_string_value(interface,"busid",dp->port,30);
				json_t *obj = json_object_get(index,"busid");
				if(obj)
				{
					const char *kv = json_string_value(obj);
					int len = json_string_length(obj);
				
					memcpy(current->port,kv,len);
					// CRITICAL FIX: Explicitly null-terminate the string
   					 current->port[len] = '\0';
				}
				
				printf("BUS id address: %s |%s| %d\n",current->port,__FILE__,__LINE__);
				
				
				json_t *jIP = json_object_get(index,"ipv4");
				if(!jIP)
				{
					printf("ipv4 not configured %s | %d\n",__FILE__,__LINE__);
					exit(1);
				}
				// CRITICAL SAFETY CHECK: Prevent NULL pointer crashes
				if (!json_is_string(jIP))
				{
					fprintf(stderr, "[ERROR] 'ipv4' is in the config file but is NOT a valid string type! %s | %d\n", __FILE__, __LINE__);
					exit(1);
				}
				else
				{
					struct sockaddr_in  saddr;
					const char *ipv4 =json_string_value(jIP);
					inet_aton(ipv4,&saddr.sin_addr);
					current->ipv4 = saddr.sin_addr.s_addr;
					printf("IPV4 address: %s %u |%s| %d\n",ipv4,current->ipv4,__FILE__,__LINE__);
				}
				current->rx = dpdk_config__get_int( index, "RX", 0, 0);
				current->tx = dpdk_config__get_int( index, "TX", 0, 0);

				printf("Recvier transmmit queue rx=%u tx=%u %s|%d\n",current->rx,current->tx,__FILE__,__LINE__);

				for(int i=0;i<5;i++)
				{
					current->rx_queue[i]=i;
					current->tx_queue[i]=i;
				}
				current->mbufsize = dpdk_config__get_int( index, "MBUFSIZE", 0, 0);
				current->mbufcachesize = dpdk_config__get_int( index, "cachesize", 0, 0);

				printf("Memory BUF  mbufsize=%u mbufcachesize=%u %s|%d\n",current->mbufsize,current->mbufcachesize,__FILE__,__LINE__);

				current->brustsize = dpdk_config__get_int( index, "Ringbrustsize", 0, 0);

				printf("Brust size  brustsize=%u  %s|%d\n",current->brustsize,__FILE__,__LINE__);

				current->rxdesc = dpdk_config__get_int( index, "rxdesc", 0, 0);
				current->txdesc = dpdk_config__get_int( index, "txdesc", 0, 0);

				printf("RX TX DESCRIP rx=%u tx=%u %s|%d\n",current->rxdesc,current->txdesc,__FILE__,__LINE__);

				current->mtusize = dpdk_config__get_int( index, "MTUsize", 0, 0);

				printf("mtusize=%u %s|%d\n",current->mtusize,__FILE__,__LINE__);
				
				json_t *jDestip = json_object_get(index,"Destip");
				if(!jDestip)
				{
					printf("Destip not configured %s | %d\n",__FILE__,__LINE__);
					exit(1);
				}
				// CRITICAL SAFETY CHECK: Prevent NULL pointer crashes
				if (!json_is_string(jDestip))
				{
					fprintf(stderr, "[ERROR] 'Destip' is in the config file but is NOT a valid string type! %s | %d\n", __FILE__, __LINE__);
					exit(1);
				}
				else
				{
					struct sockaddr_in  saddr;
					const char *Destip =json_string_value(jDestip);
					inet_aton(Destip,&saddr.sin_addr);
					current->Destip = saddr.sin_addr.s_addr;
					printf("Dest IPV4 address: %s %u |%s| %d\n",Destip,current->Destip,__FILE__,__LINE__);
				}
				
				json_t *tx_obj = json_object_get(index,"TXbusid");
				if(tx_obj)
				{
					const char *kv = json_string_value(tx_obj);
					int len = json_string_length(tx_obj);
				
					memcpy(current->tx_port,kv,len);
					// CRITICAL FIX: Explicitly null-terminate the string
   					 current->tx_port[len] = '\0';
				}
				
				printf("TX BUS id address: %s |%s| %d\n",current->tx_port,__FILE__,__LINE__);
				
				//printf("IPV4 address: %d |%s| %d\n",dp->ipv4,__FILE__,__LINE__);

					//printf("total array in index =%d : %d |%s | %d\n",1,dp->ipv4,__FILE__,__LINE__);
				//printf("address dp=%p current=%p |%d\n",dp,current,__LINE__);
				current->Next = NULL;
				if(dp == NULL)
				{
					dp =current;
					//printf("address dp=%p current=%p |%d\n",dp,current,__LINE__);
				}
				else
				{
					dpdk_app_t *tmp =dp;
					while(tmp->Next != NULL)
					{
						//printf("address dp=%p tmp=%p Next=%p |%d\n",dp,tmp,dp->Next,__LINE__); 
						tmp =tmp->Next;
					}
					tmp->Next = current;
					dp = tmp;
					//printf("address dp=%p current=%p next=%p |%d\n",dp,current,dp->Next,__LINE__);
				}
			}
			json_t * Enable = json_object_get(config,"Enablelogs");
			if(!Enable)
			{
				printf("please configure Enablelogs %s |%d\n",__FILE__,__LINE__);
				exit(0);
			}
			else
			{	
				dp->Enablelogs = dpdk_config__get_int( config, "Enablelogs", 0, 0);
				printf("Enable logs =%d\n", dp->Enablelogs);	
			}		
			json_t * Trace = json_object_get(config,"TraceEnable");
			if(!Trace)
			{
				printf("please configure TraceEnable %s |%d\n",__FILE__,__LINE__);
				exit(0);
			}
			else
			{
				dp->TraceEnable = dpdk_config__get_int( config, "TraceEnable", 0, 0);
				printf("TraceEnable logs =%d\n", dp->TraceEnable);	
			}	

			strcpy(dp->folder,"logs");
			strcpy(dp->stats_folder,"perf");
			strcpy(dp->path_folder,"print");
            
			dp->dp_stats.last_received 			 = 0;
			dp->dp_stats.total_received 		 = 0;
			dp->dp_stats.last_dequeued  		 = 0;
			dp->dp_stats.total_dequeued 		 = 0;
			dp->dp_stats.last_enqueued  		 = 0;
			dp->dp_stats.total_enqueued 		 = 0;
			dp->dp_stats.last_sent      		 = 0;
			dp->dp_stats.total_sent     		 = 0;
			dp->dp_stats.last_proceed   		 = 0;
			dp->dp_stats.total_proceed  		 = 0;
			dp->dp_stats.arp_request_received 	 = 0;
			dp->dp_stats.arp_responce_sent 		 = 0;
			dp->dp_stats.last_dropped			 = 0;
			dp->dp_stats.total_dropped 			 = 0;
			dp->dp_stats.la_tx_dropped 			 = 0;
			dp->dp_stats.tx_dropped 			 = 0;	
			dp->dp_stats.la_p_dropped			 = 0;
			dp->dp_stats.p_dropped				 = 0;

	}
}


//void perf_stats_handler(dpdk_app_t *perf)
//int perf_stats_handler(void *arg)
//{
	//struct rte_eth_stats stats;
	//stats->
	//printf("perf stats handler\n");

	//while(1)
	//{
		//printf("  rx_nombuf   =%lu  total rx_nombuf  =%lu\n",stats.rx_nombuf ,stats.rx_nombuf);
		//printf("  Dropped Packets:  %lu     ierrors =%lu oerrors=%lu\n", stats.imissed,stats.ierrors,stats.oerrors); 
		//printf("  arp_recive   =%lu  total arp_sent  =%lu\n",dp->dp_stats.arp_request_received ,dp->dp_stats.arp_responce_sent);
		//printf("last recvied   =%lu  total recv     =%lu\n",dp->dp_stats.total_received - dp->dp_stats.last_received,dp->dp_stats.total_received);
		//printf("last dequeued  =%lu  total dequeued =%lu\n",dp->dp_stats.total_dequeued - dp->dp_stats.last_dequeued,dp->dp_stats.total_dequeued);
		//printf("last enqueued  =%lu  total enqueued =%lu\n",dp->dp_stats.total_enqueued - dp->dp_stats.last_enqueued,dp->dp_stats.total_enqueued);
		//printf("last sent      =%lu  total sent  	=%lu\n",dp->dp_stats.total_sent - dp->dp_stats.last_sent,dp->dp_stats.total_sent);
		//printf("last process   =%lu  total process  =%lu\n",dp->dp_stats.total_proceed - dp->dp_stats.last_proceed,dp->dp_stats.total_proceed);

		//printf("\n");
		//dp->dp_stats.last_received = dp->dp_stats.total_received;
		//dp->dp_stats.last_dequeued = dp->dp_stats.total_dequeued;
		//dp->dp_stats.last_enqueued = dp->dp_stats.total_enqueued;
		//dp->dp_stats.last_sent     = dp->dp_stats.total_sent;
		//dp->dp_stats.last_proceed     = dp->dp_stats.total_proceed;

		//sleep(4); 
	//}
//}

void *perfomance_logs(void *arg)
{
	char time_buffer[40];
	struct rte_eth_stats stats;
	//stats->
	printf("perf stats handler\n");
	rte_eth_stats_get(dp->portid, &stats);
	while(1)
	{
		memset(time_buffer,0,sizeof(time_buffer));
		time_t raw_time = time(NULL);
		struct tm *timestamp = localtime(&raw_time); 
		sprintf(time_buffer,"%02d-%02d-%d:%02d:%02d:%02d",timestamp->tm_mday,timestamp->tm_mon +1,timestamp->tm_year + 1900,timestamp->tm_hour,timestamp->tm_min,timestamp->tm_sec);

		dpdk_perf_stats_log(dp->s_fd,"%s",time_buffer);
		dpdk_perf_stats_log(dp->s_fd,"rx_nombuf   =%lu  total rx_nombuf  =%lu",stats.rx_nombuf ,stats.rx_nombuf);
		dpdk_perf_stats_log(dp->s_fd,"Dropped Packets:%lu  ierrors =%lu oerrors=%lu", stats.imissed,stats.ierrors,stats.oerrors); 
		dpdk_perf_stats_log(dp->s_fd,"arp_recive    =%lu  total arp_sent   =%lu", dp->dp_stats.arp_request_received ,dp->dp_stats.arp_responce_sent);
		dpdk_perf_stats_log(dp->s_fd,"last recvied  =%lu  total recv       =%lu",dp->dp_stats.total_received - dp->dp_stats.last_received,dp->dp_stats.total_received);
		dpdk_perf_stats_log(dp->s_fd,"last dequeued =%lu  total dequeued   =%lu",dp->dp_stats.total_dequeued - dp->dp_stats.last_dequeued,dp->dp_stats.total_dequeued);
		dpdk_perf_stats_log(dp->s_fd,"last enqueued =%lu  total enqueued   =%lu",dp->dp_stats.total_enqueued - dp->dp_stats.last_enqueued,dp->dp_stats.total_enqueued);
		dpdk_perf_stats_log(dp->s_fd,"last sentt    =%lu  total sent  	  =%lu",dp->dp_stats.total_sent - dp->dp_stats.last_sent,dp->dp_stats.total_sent);
		dpdk_perf_stats_log(dp->s_fd,"last process  =%lu  total process    =%lu",dp->dp_stats.total_proceed - dp->dp_stats.last_proceed,dp->dp_stats.total_proceed);
		dpdk_perf_stats_log(dp->s_fd,"rx dropped    =%lu  total dropped    =%lu",dp->dp_stats.total_dropped - dp->dp_stats.last_dropped,dp->dp_stats.total_dropped);
		dpdk_perf_stats_log(dp->s_fd,"tx dropped    =%lu  total dropped    =%lu",dp->dp_stats.la_tx_dropped - dp->dp_stats.tx_dropped,dp->dp_stats.la_tx_dropped);
		dpdk_perf_stats_log(dp->s_fd,"pac dropped    =%lu total dropped    =%lu",dp->dp_stats.la_p_dropped - dp->dp_stats.p_dropped,dp->dp_stats.la_p_dropped);
		dpdk_perf_stats_log(dp->s_fd,"-------------------------------------------------------------------------------------------");
		dp->dp_stats.last_received = dp->dp_stats.total_received;
		dp->dp_stats.last_dequeued = dp->dp_stats.total_dequeued;
		dp->dp_stats.last_enqueued = dp->dp_stats.total_enqueued;
		dp->dp_stats.last_sent     = dp->dp_stats.total_sent;
		dp->dp_stats.last_proceed  = dp->dp_stats.total_proceed;
		dp->dp_stats.last_dropped  =dp->dp_stats.total_dropped;
		dp->dp_stats.tx_dropped    =dp->dp_stats.la_tx_dropped;
		dp->dp_stats.p_dropped     = dp->dp_stats.la_p_dropped;
		usleep(1000000);
		//sleep(1);
	}
}

/*
void launch(int worker_lcore_id)
{
	int ret=0;
	dpdk_app_t *interface = dp;
	while(interface != NULL)
	{
		for(int i=0;i<interface->rx;i++)
		{
			
			//dpdk_app_t *interface = (dpdk_app_t *)malloc(sizeof(dpdk_app_t));
			interface->rx_queueid = i;

			ret = rte_eal_remote_launch(arp_recevie,interface,worker_lcore_id++);
			printf("queue=%d lcore=%d ret=%d\n",i,worker_lcore_id,ret);
			printf("launched arp recevie: %d worker_lcore_id = %d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd,"launched arp recevie: %d worker_lcore_id = %d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);

		}
		for(int i=0;i<interface->rx;i++)
		{
			interface->workerid = i;
			rte_eal_remote_launch(arp_worker,interface,worker_lcore_id++);
			printf("launched arp worker: %d worker_lcore_id =%d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd,"launched arp worker: %d worker_lcore_id =%d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);
		}
		for(int i=0;i<interface->tx;i++)
		{
			interface->tx_queueid = i;
			rte_eal_remote_launch(arp_send,interface,worker_lcore_id++);
			printf(" launched arp send: %d worker_lcore_id=%d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd," launched arp send: %d worker_lcore_id=%d |%s|%d\n",interface->portid,worker_lcore_id,__FILE__,__LINE__);
		}
	// rte_eal_remote_launch(perf_stats_handler,NULL,worker_lcore_id++);  
	usleep(5000);
	interface = interface->Next;
	}
}*/



void app__signalhandler( int signum)
{
	//running = 0;
	printf("\nCaught signal %d, coming out...in %d seconds.\n", signum, 0);
	//dpdk_perf_stats_log(dp->p_fd,"Caught signal %d, coming out...in %d seconds", signum, 0);
	close(dp->s_fd);
	close(dp->p_fd);
	//dpdk_pkt__close_pcap( g_dumper);
	dpdk_close_dumper_packet(dp->dumper);
	
	exit(0);
}


int main(int argc,char **argv)
{
	int ret;
	int worker_lcore_id=0;
	
	ret = rte_eal_init(argc,argv);
	if(ret < 0)
	{
		//rte_exit(EXIT_FAILURE,"eal not initlized | %d\n",__LINE__);
		rte_exit(EXIT_FAILURE,"EAL init failed: %s (rte_errno=%d) line=%d\n",rte_strerror(rte_errno),rte_errno, __LINE__); 
	}
	
	
	// int nb_ports = rte_eth_dev_count_avail();
	// if(nb_ports == 0)
	// {
		// rte_exit(EXIT_FAILURE,"NO ETHERNET PORTS ");
	// }
	// else
	// {
		// printf("Total configured ports=%d\n",nb_ports);
	// }
	sleep(2);
	
	signal( SIGINT, app__signalhandler);		// Ctrl + C
	signal( SIGQUIT, app__signalhandler);		// Ctrl + \ 			//
	signal( SIGTERM, app__signalhandler);		// shell command kill generates SIGTERM by default

	dpe_configure("dpdk.json"); 
	if(dp->Enablelogs == 1)
	{
		folder_create(); 
		pthread_t print_id;

		ret =pthread_create(&print_id,NULL,(void *)dpdk_log_thread,NULL);
		if(ret <0)
		{
			printf("pthread exits=%ld|%s|%d\n",print_id,__FILE__,__LINE__);
			exit(0);
		}
		pthread_join(print_id,NULL);
		
	}
	if(dp->TraceEnable == 1)
	{
		dpdk_open_dumper_packet();
	}

	worker_lcore_id = rte_get_next_lcore(-1,1,0);
	if(worker_lcore_id == RTE_MAX_LCORE)
	{
		rte_exit(EXIT_FAILURE,"worker core exitied\n");
	}
	
	
	dpdk_app_t *dpe = dp;
	//printf("\t------- address dpe=%p Next=%p ----\n",dpe,dpe->Next); 
	while(dpe)
	{
		printf("\t------- address dpe=%p ----\n",dpe);
		printf("pci=%s\n",dpe->port);
		ret = rte_eth_dev_get_port_by_name(dpe->port,&dpe->portid);
		if (ret < 0)
		{
			rte_exit(EXIT_FAILURE,"port lookup failed pci=%s ret=%d line=%d\n",dpe->port,ret,__LINE__);
		}

		printf("port=%u\n", dpe->portid);

		ret = rte_eth_dev_is_valid_port( dpe->portid);
		
		if( ret < 0)
		{
			printf("Error: rte_eth_dev_is_valid_port failed for BusId=%d\n", dpe->portid);
			exit(0);
		}
		
		

		struct rte_eth_dev_info dev_info;
		struct rte_eth_conf port_conf; 
		
		memset( &dev_info, 	0, sizeof(struct rte_eth_dev_info));
		memset( &port_conf, 0, sizeof(struct rte_eth_conf));

		ret = rte_eth_dev_info_get(dpe->portid,&dev_info);
		if(ret < 0)
		{
			rte_exit(EXIT_FAILURE,"dev info failed | %d\n",__LINE__);
		}
		printf("RSS offloads: 0x%lx\n", dev_info.flow_type_rss_offloads);

		port_conf.lpbk_mode = 0;

		port_conf.rxmode.mq_mode 				= RTE_ETH_MQ_RX_RSS; // RTE_ETH_MQ_RX_RSS

		port_conf.rx_adv_conf.rss_conf.rss_hf		= RTE_ETH_RSS_IP | RTE_ETH_RSS_UDP | RTE_ETH_RSS_TCP;

		port_conf.txmode.mq_mode 				= RTE_ETH_MQ_TX_NONE;
		if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
		{
			port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;
		}

		port_conf.rx_adv_conf.rss_conf.rss_hf &= dev_info.flow_type_rss_offloads;
		
		
		char mempool_name[30];
		for(int i=0;i<dpe->rx;i++)
		{
			memset( mempool_name, 0, sizeof(mempool_name));
			sprintf( mempool_name, "mempool--%d-%d", i,dpe->portid);
			dpe->mbuf_pool[i] = rte_pktmbuf_pool_create(mempool_name,dpe->mbufsize,dpe->mbufcachesize,0,RTE_MBUF_DEFAULT_BUF_SIZE,rte_socket_id());
			if(!dpe->mbuf_pool)
			{
				rte_exit(EXIT_FAILURE,"MBUFPOOL CREATION FAILED | %d\n",__LINE__);
			}
			else
			{
				printf(" memepool=%s create=%p mbufsize=%d mbufcachesize=%u |%s|%d\n",mempool_name,dpe->mbuf_pool[i],dpe->mbufsize,dpe->mbufcachesize,__FILE__,__LINE__);
				dpdk_perf_stats_log(dp->p_fd," memepool=%s create=%p mbufsize=%d mbufcachesize=%u |%s|%d\n",mempool_name,dpe->mbuf_pool[i],dpe->mbufsize,dpe->mbufcachesize,__FILE__,__LINE__);
				
			}
		}
		
		ret = rte_eth_dev_configure(dpe->portid,dpe->rx,dpe->tx,&port_conf);
		if( ret < 0)
		{
			rte_exit(EXIT_FAILURE,"ETH DEV CONFIGURATION FAILED | %d\n",__LINE__);
		}else
		{
			printf("rte dev configure RX=%d TX=%d |%s|%d\n",dpe->rx,dpe->tx,__FILE__,__LINE__);
			
			dpdk_perf_stats_log(dp->p_fd,"rte dev configure RX=%d TX=%d |%s|%d\n",dpe->rx,dpe->tx,__FILE__,__LINE__);
			
		}
		
		// uint16_t tx_port =0;
		// ret = rte_eth_dev_get_port_by_name(dpe->tx_port,&tx_port);
		// if (ret < 0)
		// {
			// rte_exit(EXIT_FAILURE,"port lookup failed pci=%s ret=%d line=%d\n",dpe->port,ret,__LINE__);
		// }
		struct rte_ether_addr mac_addr;
		//struct rte_ether_addr tx_mac_addr;
		
		memset(&mac_addr,0,sizeof(struct rte_ether_addr));
		//memset(&tx_mac_addr,0,sizeof(struct rte_ether_addr));
		
		ret = rte_eth_macaddr_get(dpe->portid,&mac_addr);
		if(ret < 0)
		{
			rte_exit(EXIT_FAILURE,"Failed to get mac_addr| %d\n",__LINE__); 
		}
		
		// ret = rte_eth_macaddr_get(tx_port,&tx_mac_addr);
		// if(ret < 0)
		// {
			// rte_exit(EXIT_FAILURE,"Failed to get mac_addr| %d\n",__LINE__); 
		// }
		
		else
		{
			memcpy(dpe->macaddr,&mac_addr.addr_bytes,6);
			//memcpy(dpe->tx_macaddr,&tx_mac_addr.addr_bytes,6);
			printf("PORT :%d SRC_MAC :%02X:%02X:%02X:%02X:%02X:%02X \n",dpe->portid,dpe->macaddr[0],dpe->macaddr[1],
																	dpe->macaddr[2],dpe->macaddr[3],dpe->macaddr[4],dpe->macaddr[5]
																	);
			
			dpdk_perf_stats_log(dp->p_fd,"PORT :%d  SRC_MAC :%02X:%02X:%02X:%02X:%02X:%02X ",dpe->portid,dpe->macaddr[0],
																dpe->macaddr[1],dpe->macaddr[2],dpe->macaddr[3],dpe->macaddr[4],dpe->macaddr[5]
																);
		}														
		uint16_t mtu_current=1500;
		ret = rte_eth_dev_set_mtu(dpe->portid,dpe->mtusize); 
		if(ret < 0)
		{
			rte_exit(EXIT_FAILURE,"MTU set 1500 | %d\n",__LINE__); 
		}



		//for port 0 or pci address 0 responce
		for(int i=0 ;i< dpe->rx;i++)
		{
			//ret = rte_eth_rx_queue_setup(dp->portid,dp->rx_queue[i],512, rte_eth_dev_socket_id(dp->portid),NULL,dp->mbuf_pool[i]);
			ret = rte_eth_rx_queue_setup(dpe->portid,i,512, rte_eth_dev_socket_id(dpe->portid),NULL,dpe->mbuf_pool[i]);
			if( ret <0)
			{
				rte_exit(EXIT_FAILURE,"rx_queue setp=%d | %d\n",dpe->rx_queue[i],__LINE__);
				exit(0); 
			}
			else
			{
				printf("rx=%d queue setup =%d |%s|%d\n",dpe->portid,dpe->rx_queue[i],__FILE__,__LINE__);
				dpdk_perf_stats_log(dp->p_fd,"rx=%d queue setup =%d |%s|%d",dpe->portid,dpe->rx_queue[i],__FILE__,__LINE__);
			}
		}

		for(int i=0; i<dpe->tx;i++)
		{
			ret = rte_eth_tx_queue_setup(dpe->portid,i,512,rte_eth_dev_socket_id(dpe->portid),NULL);
			if( ret <0)
			{
				rte_exit(EXIT_FAILURE,"tx_queue setp=%d | %d\n",dpe->rx_queue[i],__LINE__);
				exit(0); 
			}
			else
			{
				printf("tx=%d queue setup =%d |%s|%d\n",dpe->portid,dpe->rx_queue[i],__FILE__,__LINE__);
				dpdk_perf_stats_log(dp->p_fd,"tx=%d queue setup =%d |%s|%d",dpe->portid,dpe->rx_queue[i],__FILE__,__LINE__);
			}
		}

		//rte_eth_rx_queue_setup(dp->portid,dp->rx_queue[1],512, rte_eth_dev_socket_id(dp->portid),NULL,dp->mbuf_pool[1]);
		//rte_eth_tx_queue_setup(dp->portid,dp->tx_queue[1],512,rte_eth_dev_socket_id(dp->portid),NULL);
		

		//dp->rxring_buffer = rte_ring_create("rxring1",rte_align32pow2(dp->mbufsize),rte_socket_id(),RING_F_SP_ENQ | RING_F_SC_DEQ); 
		char name[RTE_RING_NAMESIZE];
		for(int i=0;i<dpe->rx;i++)
		{
			//snprintf(name,13,"rxring%d%d",i,dpe->portid); // before for single interface 14/07/26
			sprintf(name,"rxring%d%d",i,dpe->portid);
			dpe->rxring_buffer[i] = rte_ring_create(name,rte_align32pow2(dpe->mbufsize),rte_socket_id(),RING_F_MP_RTS_ENQ | RING_F_MC_RTS_DEQ); 
			if (!dpe->rxring_buffer) 
			{
				printf("ring create failed: %s | %d\n",rte_strerror(rte_errno),__LINE__); 
				rte_exit(EXIT_FAILURE,"ring create failed| %d\n",__LINE__); 
				exit(EXIT_FAILURE);
			}
			else
			{
				printf("RX rte_ring_create =%p name =%s |%s|%d\n",dpe->rxring_buffer[i],name,__FILE__,__LINE__);
				dpdk_perf_stats_log(dp->p_fd,"RX rte_ring_create =%p name =%s |%s|%d\n",dpe->rxring_buffer[i],name,__FILE__,__LINE__);
			}
			
		}

		//dp->txring_buffer = rte_ring_create("txring1",rte_align32pow2(dp->mbufsize),rte_socket_id(),RING_F_SP_ENQ | RING_F_SC_DEQ); 
		char tx_name[RTE_RING_NAMESIZE];
		for(int i=0;i<dpe->tx;i++)
		{
			//snprintf(tx_name,12,"txring%d%d",i,dpe->portid); // before for single interface 14/07/26
			sprintf(tx_name,"txring%d%d",i,dpe->portid); 
			dpe->txring_buffer[i] = rte_ring_create(tx_name,rte_align32pow2(dpe->mbufsize),rte_socket_id(),RING_F_MP_RTS_ENQ | RING_F_MC_RTS_DEQ); 
			if (!dpe->txring_buffer) 
			{
				printf("ring create failed: %s | %d\n",rte_strerror(rte_errno),__LINE__);
				exit(EXIT_FAILURE);
			}
			else
			{
				printf("Tx rte_ring_create =%p name =%s |%s|%d\n",dpe->txring_buffer[i],tx_name,__FILE__,__LINE__);
				dpdk_perf_stats_log(dp->p_fd,"Tx rte_ring_create =%p name =%s |%s|%d\n",dpe->txring_buffer[i],tx_name,__FILE__,__LINE__);
			}
		}
		
		//printf("1.create ring rx=%p tx=%p | %d\n",dp->rxring_buffer[0],dp->txring_buffer[0],__LINE__);
		//printf("2.create ring rx=%p tx=%p | %d\n",dp->rxring_buffer[1],dp->txring_buffer[1],__LINE__);
		
	
		
		printf("------Before start: avail=%u in_use=%u |%s|%d\n",rte_mempool_avail_count(dpe->mbuf_pool[0]),rte_mempool_in_use_count(dpe->mbuf_pool[0]),__FILE__,__LINE__);
		int res_dev = rte_eth_dev_start(dpe->portid);
		if(res_dev < 0)
		{
			rte_exit(EXIT_FAILURE,"FAILED TO START PORT=%d |%s|%d\n",dpe->portid,__FILE__,__LINE__);
			exit(1);
		}
		else
		{
			printf("rte_eth_dev_start=%d port id=%d |%s %d\n",res_dev,dpe->portid,__FILE__,__LINE__);
		}
		printf("-----after start: avail=%u in_use=%u |%s|%d \n",rte_mempool_avail_count(dpe->mbuf_pool[0]),rte_mempool_in_use_count(dpe->mbuf_pool[0]),__FILE__,__LINE__);
		
		
		int res =rte_eth_promiscuous_enable(dpe->portid);
		if (res != 0) {
			printf("WARNING: Failed to enable promiscuous mode on app_res->port 0: %s\n", rte_strerror(-res));
			exit(EXIT_FAILURE);
		} else {
			printf("SUCCESS: Promiscuous mode enabled on port=%d |%s|%d \n",dpe->portid,__FILE__,__LINE__);
		}

		struct rte_eth_link link;
		memset(&link, 0, sizeof(link));
		rte_eth_link_get_nowait(dpe->portid, &link);
		printf("Port %u Link status: %s | Speed: %u Mbps |%s|%d\n", 
		dpe->portid, (link.link_status ? "UP" : "DOWN"), link.link_speed,__FILE__,__LINE__);
		
		//usleep(100000);
	dpe = dpe->Next;	
	}
	
	dpdk_app_t * check = dp;
	while(check)
	{
		dpe = dp;
		while(dpe)
		{
			if(strcmp(dpe->tx_port,check->port)==0)
			{
				dpe->tx_interface = check;
				printf("PORT :%d SRC_MAC :%02X:%02X:%02X:%02X:%02X:%02X DST_MAC :%02X:%02X:%02X:%02X:%02X:%02X\n",dpe->portid,dpe->macaddr[0],dpe->macaddr[1],
																	dpe->macaddr[2],dpe->macaddr[3],dpe->macaddr[4],dpe->macaddr[5],
																	dpe->tx_interface->macaddr[0],dpe->tx_interface->macaddr[1],
																	dpe->tx_interface->macaddr[2],dpe->tx_interface->macaddr[3],
																	dpe->tx_interface->macaddr[4],dpe->tx_interface->macaddr[5]
																	);
																	
																	printf("\t\t-----comapre current vs TX----\n");

			
				dpdk_perf_stats_log(dp->p_fd,"PORT :%d  SRC_MAC :%02X:%02X:%02X:%02X:%02X:%02X DST_MAC :%02X:%02X:%02X:%02X:%02X:%02X",dpe->portid,dpe->macaddr[0],
																dpe->macaddr[1],dpe->macaddr[2],dpe->macaddr[3],dpe->macaddr[4],dpe->macaddr[5],
																dpe->tx_interface->macaddr[0],dpe->tx_interface->macaddr[1],
																dpe->tx_interface->macaddr[2],dpe->tx_interface->macaddr[3],
																dpe->tx_interface->macaddr[4],dpe->tx_interface->macaddr[5]
																	);
			}
			dpe = dpe->Next;
		}
		check = check->Next;
	}
	
	dpe = dp;
	while(dpe)
	{
		for(int i=0;i<dpe->rx;i++)
		{
			
			//dpdk_app_t *interface = (dpdk_app_t *)malloc(sizeof(dpdk_app_t));
			dpe->rx_queueid = i;

			ret = rte_eal_remote_launch(arp_recevie,dpe,worker_lcore_id++);
			printf("queue=%d lcore=%d ret=%d\n",i,worker_lcore_id,ret);
			printf("launched arp recevie: %d worker_lcore_id = %d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd,"launched arp recevie: %d worker_lcore_id = %d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);

		}
		for(int i=0;i<dpe->rx;i++)
		{
			dpe->workerid = i;
			rte_eal_remote_launch(arp_worker,dpe,worker_lcore_id++);
			printf("launched arp worker: %d worker_lcore_id =%d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd,"launched arp worker: %d worker_lcore_id =%d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);
		}
		for(int i=0;i<dpe->tx;i++)
		{
			dpe->tx_queueid = i;
			rte_eal_remote_launch(arp_send,dpe,worker_lcore_id++);
			printf(" launched arp send: %d worker_lcore_id=%d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);
			dpdk_perf_stats_log(dp->p_fd," launched arp send: %d worker_lcore_id=%d |%s|%d\n",dpe->portid,worker_lcore_id,__FILE__,__LINE__);
		}
		dpe = dpe->Next;	
	}
	//worker_lcore_id++;
	
	/*int req_lcore_id = rte_get_next_lcore(worker_lcore_id,1,0);
	if(req_lcore_id == RTE_MAX_LCORE)
	{
		rte_exit(EXIT_FAILURE,"worker core exitied\n");
	}
	int req_lcore_id1 = rte_get_next_lcore(req_lcore_id,1,0);
	if(req_lcore_id == RTE_MAX_LCORE)
	{
		rte_exit(EXIT_FAILURE,"worker core exitied\n");
	}
	int req_lcore_id2 = rte_get_next_lcore(req_lcore_id1,1,0);
	if(req_lcore_id == RTE_MAX_LCORE)
	{
		rte_exit(EXIT_FAILURE,"worker core exitied\n");
	}*/
	
	
	//launch(worker_lcore_id);
		
	
	printf("Main thread waiting on core %u worker_lcore_id=%d. Press Ctrl+C to terminate application.\n", rte_lcore_id(),worker_lcore_id);
	
	//sleep(5);
	//arp_send_request(dp);
	//printf("arp request is sent\n");
	
	if(dp->Enablelogs == 1)
	{
		pthread_t thread_id;

		ret =pthread_create(&thread_id,NULL,&perfomance_logs,NULL);
		if(ret <0)
		{
			printf("pthread exits=%ld|%s|%d\n",thread_id,__FILE__,__LINE__);
			exit(0);
		}
		//pthread_join(thread_id,NULL);
	}
	while(1){
		 usleep(10000); 
	}
	

	//struct rte_eth_stats stats;
	

	//while(1)
	//{
		//if(rte_eth_stats_get(dp->portid,&stats) == 0)
		//{
			//printf("ipackets =%lu \t opackets=%lu\n",stats.ipackets,stats.opackets);
			//printf("ipackets =%lu \t opackets=%lu\n",stats.ierrors,stats.oerrors);
			//printf("ipackets =%lu \t opackets=%lu\n",stats.imissed,stats.rx_nombuf);
		//}
		//rte_delay_ms(1000);
	//}
	dpdk_close_dumper_packet(dp->dumper);
	close_folder();
	return 0;
}