import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SSHJTest {

    public static void main(String[] args) throws IOException {
        final SSHClient ssh = new SSHClient();
        //NOTE: known_hosts doesn't exist on Solaris, so this is useless:
        ssh.loadKnownHosts();

        ssh.addHostKeyVerifier("70:be:ce:13:ab:58:ab:b3:00:e8:5b:28:58:00:cb:2a"); //1
        ssh.addHostKeyVerifier("9c:97:8f:30:1f:c7:53:b0:bc:64:01:22:cc:94:33:8a"); //2
        ssh.addHostKeyVerifier("c3:27:c8:7c:7e:10:63:9a:f0:8d:db:a9:c0:d8:b0:bd"); //3
        ssh.addHostKeyVerifier("ac:9f:93:22:f9:4d:a3:18:67:95:05:23:d0:be:38:8e"); //4
        ssh.addHostKeyVerifier("06:18:e7:eb:0e:78:d0:6b:29:9f:ff:d1:ab:fa:0c:c0"); //1-7
        ssh.addHostKeyVerifier("eb:2e:9f:ab:bd:50:56:da:41:21:5d:02:f3:89:31:e7"); //1-18
        ssh.addHostKeyVerifier("d3:4c:01:ef:fe:9f:a6:bc:dd:be:03:e8:1d:ef:38:6a"); //2-7
        ssh.addHostKeyVerifier("de:ef:0a:d6:38:c6:1e:35:75:60:3c:95:ba:87:99:c4"); //2-18
        ssh.addHostKeyVerifier("e0:de:aa:6d:3d:fb:98:7b:d1:a7:28:14:51:b8:99:e3"); //3-7
        ssh.addHostKeyVerifier("5e:1b:26:0b:50:ab:48:89:67:ba:02:80:b7:a2:a3:d0"); //3-18
        ssh.addHostKeyVerifier("87:55:99:24:92:11:9b:f4:cb:95:95:d3:69:c7:41:f6"); //4-7
        ssh.addHostKeyVerifier("50:9f:e1:0d:1d:71:6f:40:7b:bc:f0:df:50:0b:5a:e9"); //4-18

        if (args.length>0)
            ssh.connect(args[0]);
        else
            ssh.connect("icdatasrv3-7");

        try {
            ssh.authPublickey("team7");
            final Session session = ssh.startSession();
            try {
                final Command cmd = session.exec("ls -lh");
                System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
                cmd.join(5, TimeUnit.SECONDS);
                System.out.println("\n** exit status: " + cmd.getExitStatus());
            } finally {
                session.close();
            }
        } finally {
            ssh.disconnect();
        }
    }

}
