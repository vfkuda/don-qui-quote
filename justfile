
runs:
    cargo r --bin=quote_server 

runc1:
    cargo r --bin=quote_client --release -- --server-addr 127.0.0.1:7000 --udp-port 34254 --tickers-file ./client1.tic

runc2:
    cargo r --bin=quote_client --release -- --server-addr 127.0.0.1:7000 --udp-port 34255 --tickers-file ./client2.tic
