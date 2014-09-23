package MarketPriceBackFeedNotification;

public interface IDataReader<V> {
    
    public V getData() throws Exception;
    
}