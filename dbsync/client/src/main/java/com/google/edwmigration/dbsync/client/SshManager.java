package com.google.edwmigration.dbsync.client;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.jcraft.jsch.AgentConnector;
import com.jcraft.jsch.AgentIdentityRepository;
import com.jcraft.jsch.AgentProxyException;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.MyUserAuthPublicKey;
import com.jcraft.jsch.SSHAgentConnector;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.Slf4jLogger;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import org.anarres.jdiagnostics.ProductMetadata;
import org.anarres.jdiagnostics.ProductMetadata.ModuleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SshManager {

    public static class MyUserInfo implements UserInfo, UIKeyboardInteractive {

        public String getPassword() {
            return "passwordHere";
        }

        public boolean promptYesNo(String str) {
            return true;
        }

        public String getPassphrase() {
            return null;
        }

        public boolean promptPassphrase(String message) {
            return false;
        }

        public boolean promptPassword(String message) {
            return true;
        }

        public void showMessage(String message) {
            System.out.println(message);
        }

        public String[] promptKeyboardInteractive(String destination,
            String name,
            String instruction,
            String[] prompt,
            boolean[] echo) {

            System.out.println("destination: " + destination);
            System.out.println("name: " + name);
            System.out.println("instruction: " + instruction);
            System.out.println("prompt.length: " + prompt.length);

            String[] str = new String[1];

            if (prompt[0].contains("Password:")) {
                str[0] = getPassword();
            } else if (prompt[0].contains("Verification code: ")) {
                try {
                    str[0] = "foo"; // PasswordUtils.verify_code("CODEHERE");
                } catch (RuntimeException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                str = null;
            }

            return str;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SshManager.class);

    private final JSch ssh = new JSch();
    private final InetSocketAddress address;
    private final String user;

    public SshManager(InetSocketAddress address, String user) throws AgentProxyException {
        this.ssh.setInstanceLogger(new Slf4jLogger());
        AgentConnector agentConnector = new SSHAgentConnector();
        LOG.info("Agent connector is {} available {}", agentConnector,
            agentConnector.isAvailable());
        this.ssh.setIdentityRepository(new AgentIdentityRepository(agentConnector));
        this.address = Preconditions.checkNotNull(address, "Address was null.");
        this.user = Preconditions.checkNotNull(user, "User was null.");
    }

    public void addIdentity(File file) throws JSchException {
        ssh.addIdentity(file.getAbsolutePath());
    }

    private Session newSession() throws JSchException {
        LOG.info("Attempting to connect to {}@{} using ssh", user, address);
        Session session = ssh.getSession(user, address.getHostString(), address.getPort());
        session.setConfig("compression.s2c", "zlib@openssh.com,zlib,none");
        session.setConfig("compression.c2s", "zlib@openssh.com,zlib,none");
        session.setConfig("compression_level", "9");
        session.setConfig("StrictHostKeyChecking", "no");   // TODO: Change using UserInfo
        session.setConfig("userauth.publickey", MyUserAuthPublicKey.class.getName());
        // session.setConfig("ecdsa-sha2-nistp256", "com.jcraft.jsch.jce.SignatureECDSA256");
        session.setUserInfo(new MyUserInfo());
        return session;
    }

    private void upload(Session session, String targetName, ByteSource source)
        throws JSchException, SftpException, IOException {
        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        try (InputStream in = source.openBufferedStream()) {
            channel.connect();
            channel.put(in, targetName, ChannelSftp.OVERWRITE);
        } finally {
            channel.disconnect();
        }
    }

    private ChannelExec exec(Session session, String targetCommand)
        throws JSchException, IOException {
        ChannelExec channel = (ChannelExec) session.openChannel("exec");

        channel.setCommand(targetCommand);
        channel.setErrStream(System.err);

        channel.connect();

        return channel;
    }

    private ChannelExec java(Session session, String targetJar) throws JSchException, IOException {
        String targetJarQuoted = "'" + targetJar.replace("'", "'\"'\"'") + "'";
        return exec(session, "java -jar " + targetJarQuoted);
    }

    public Session setup() throws JSchException, SftpException, IOException {
        Session session = newSession();
        session.connect();

        try {
            exec(session, "mkdir -p /tmp/dbsync/");

            ProductMetadata productMetadata = new ProductMetadata();
            for (ModuleMetadata moduleMetadata : productMetadata.getModules()) {
                File jar = moduleMetadata.getJar();
                LOG.info("Product module {} jar {}", moduleMetadata, jar);
                upload(session, "/tmp/dbsync/" + jar.getName(), Files.asByteSource(jar));
            }
        } catch (SftpException | JSchException e) {
            session.disconnect();
            throw e;
        }

        return session;
    }
}

