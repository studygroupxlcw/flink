package ptest.f;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class SocTest {
    private final static Logger log = LoggerFactory.getLogger(SocTest.class);

    public static void main(String[] args) {
        byte[] readbytes = new byte[10240];
        try {
            Socket socket = new Socket("149.248.60.109", 9009);
            InputStream inputStream = socket.getInputStream();
            while (inputStream.read(readbytes) != -1){
                System.out.println(new String(readbytes));
            }

        } catch (IOException e) {
            log.error("socket io Exception", e);
        }
    }
}
