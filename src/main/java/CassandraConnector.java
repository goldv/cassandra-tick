import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Created by goldv on 7/15/2015.
 */
public class CassandraConnector {

    private Cluster cluster;
    private Session session;
    private Mapper<Tick> mapper;

    @Table(keyspace = "ticks", name = "tick")
    public static class Tick{
        private String symbol;
        private UUID inserted_at;
        private BigDecimal last;
        private int volBid;
        private BigDecimal bid;
        private BigDecimal ask;
        private int volAsk;

        public Tick(){}

        public Tick(BigDecimal ask, BigDecimal bid, UUID inserted_at, BigDecimal last, String symbol, int volAsk, int volBid) {
            this.ask = ask;
            this.bid = bid;
            this.inserted_at = inserted_at;
            this.last = last;
            this.symbol = symbol;
            this.volAsk = volAsk;
            this.volBid = volBid;
        }

        public BigDecimal getAsk() {
            return ask;
        }

        public void setAsk(BigDecimal ask) {
            this.ask = ask;
        }

        public BigDecimal getBid() {
            return bid;
        }

        public void setBid(BigDecimal bid) {
            this.bid = bid;
        }

        public void setInserted_at(UUID uuid){
            this.inserted_at = uuid;
        }

        public UUID getInserted_at() {
            return inserted_at;
        }

        public BigDecimal getLast() {
            return last;
        }

        public void setLast(BigDecimal last) {
            this.last = last;
        }

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public int getVolAsk() {
            return volAsk;
        }

        public void setVolAsk(int volAsk) {
            this.volAsk = volAsk;
        }

        public int getVolBid() {
            return volBid;
        }

        public void setVolBid(int volBid) {
            this.volBid = volBid;
        }

        @Override
        public String toString() {
            return "Tick{" +
                    "ask=" + ask +
                    ", symbol='" + symbol + '\'' +
                    ", inserted_at=" + inserted_at +
                    ", last=" + last +
                    ", volBid=" + volBid +
                    ", bid=" + bid +
                    ", volAsk=" + volAsk +
                    '}';
        }
    }


    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withQueryOptions(new QueryOptions().setFetchSize(5))
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }

        session = cluster.connect();
        mapper = new MappingManager(session).mapper(Tick.class);
    }

    public void createSchema(){
        session.execute("CREATE KEYSPACE IF NOT EXISTS ticks WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':3};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS ticks.tick (" +
                        "symbol text," +
                        "inserted_at timeuuid," +
                        "last decimal," +
                        "volBid int," +
                        "bid decimal," +
                        "ask decimal," +
                        "volAsk int," +
                        "PRIMARY KEY(symbol, inserted_at)" +
                        ") WITH CLUSTERING ORDER BY (inserted_at DESC);");
    }

    public void close() {
        cluster.close();
    }

    public Session getSession() {
        return this.session;
    }

    public List<Tick> readFromFile(String fileName) throws URISyntaxException, IOException{
        Path path = Paths.get(ClassLoader.getSystemResource(fileName).toURI());
        Reader reader = Files.newBufferedReader( path, Charset.forName("UTF-8"));
        return parseRows(reader);
    }

    public List<Tick> parseRows(Reader source) {
        try (BufferedReader reader = new BufferedReader(source)) {
            return reader.lines()
                    .skip(1)
                    .map(line -> Arrays.asList(line.split(",")))
                    .map(this::parseRow)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Tick parseRow(List<String> row){
        return new Tick(
            new BigDecimal(row.get(6) ) ,
            new BigDecimal(row.get(5) ),
            UUIDs.startOf( Long.parseLong(row.get(0))),
            new BigDecimal( row.get(0) ),
            "EUR/USD",
            Integer.parseInt( row.get(4) ),
            Integer.parseInt(row.get(5) )
        );
    }

    public List<Tick> buildTicks(){
        List<Tick> ticks = new LinkedList<Tick>();
        for( int i = 0; i < 10 ; i ++){
            Tick t = new Tick( new BigDecimal(1.2343).setScale(4, BigDecimal.ROUND_HALF_DOWN), new BigDecimal(1.244).setScale(4, BigDecimal.ROUND_HALF_DOWN), UUIDs.timeBased(), new BigDecimal(1.2453).setScale(4, BigDecimal.ROUND_HALF_DOWN),"EUR/USD",200000,250000 );
            ticks.add(t);
        }

        return ticks;
    }

    public void insertTicks(List<Tick> ticks){
        for( Tick tick : ticks){
            mapper.save(tick);
        }
    }

    public void retrieveTicks(){
        ResultSet results = this.session.executeAsync("select * from ticks.tick;").getUninterruptibly();

        Result<Tick> ticks = mapper.map(results);
        for(Tick tick : ticks){
            System.out.println("available without fetching " + results.getAvailableWithoutFetching() + " tick: " + tick);
        }
    }



    public static void main(String... args) throws Exception{
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1");
        client.createSchema();

        List<Tick> ticks = client.readFromFile("EUR.USD.csv");

        client.insertTicks(ticks);
        client.retrieveTicks();

        client.close();
    }


}
