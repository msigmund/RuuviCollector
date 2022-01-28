package fi.tkgwf.ruuvi.db;

import com.influxdb.client.WriteApiBlocking;
import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.Config;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import fi.tkgwf.ruuvi.utils.InfluxDBConverter;
import com.influxdb.client.write.Point;

public class InfluxDB2Connection implements DBConnection {

    private final InfluxDBClient influxDBClient;
    private final String bucket;
    private final String org;

    public InfluxDB2Connection() {
        this(
            Config.getInfluxUrl(),
            Config.getInflux2Token(),
            Config.getInflux2Bucket(),
            Config.getInflux2Org(),
            Config.isInfluxGzip()
        );
    }

    public InfluxDB2Connection(
        String url,
        String token,
        String bucket,
        String org,
        boolean gzip
    ) {
        this.influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
        this.bucket = bucket;
        this.org = org;

        if (gzip) {
            influxDBClient.enableGzip();
        } else {
            influxDBClient.disableGzip();
        }
    }

    @Override
    public void save(EnhancedRuuviMeasurement measurement) {
        Point point = InfluxDBConverter.toInflux2(measurement);
        WriteApiBlocking writeAPI = influxDBClient.getWriteApiBlocking();
        writeAPI.writePoint(this.bucket, this.org, point);
    }

    @Override
    public void close() {
        influxDBClient.close();
    }
}
