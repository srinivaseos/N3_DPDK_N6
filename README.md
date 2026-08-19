GTP (GPRS Tunneling Protocol) headers depending on which way the traffic is moving:
    Uplink (Request Path: N3 → N6):
               1.The packet arrives from the cell tower over the N3 interface wrapped in a GTP header.
               2.The UPF strips off the GTP header, converting it into a plain IP packet.
               3.The plain packet is routed over the N6 (Gi) interface to the DNS server/Internet.

   Downlink (Return Path: N6 → N3):
              1.The DNS response arrives at the UPF over the N6 interface as a plain IP packet.
              2.The UPF looks up the user, matches their session, and adds a GTP header back onto the packet.
              3.The wrapped GTP packet is sent over the N3 interface back to the cell tower to be delivered to the phone.
