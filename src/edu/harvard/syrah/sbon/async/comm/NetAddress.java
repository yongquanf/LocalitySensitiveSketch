/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 7, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.harvard.syrah.prp.ANSI;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.ANSI.Color;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;

/**
 * 
 * This is an Internet address and a port number as used by the Comm module.
 * 
 * 
 */
public class NetAddress implements AddressIF, Serializable {
	protected static final Log log = new Log(NetAddress.class);

	static final long serialVersionUID = 1000000001;

	private static final Color ADDR_COLOR = Color.CYAN;

	static final int UNKNOWN_PORT = -1;

	protected String hostname;
	protected byte[] byteIPAddr;
	protected int port;

	protected InetSocketAddress inetSocketAddress;
	protected InetAddress inetAddress;

	private boolean unresolvable = false;

	private boolean isWildcardAddress = false;

	NetAddress(String hostString, CB1<AddressIF> cbAddress) {
		this(hostString, null, UNKNOWN_PORT, cbAddress);
	}
	
	NetAddress(String hostString, int port, CB1<AddressIF> cbAddress) {
		this(hostString, null, port, cbAddress);		
	}

	NetAddress(int port) throws UnknownHostException {
		this(InetAddress.getLocalHost().getCanonicalHostName(),
					InetAddress.getLocalHost().getAddress(), port, null);
	}

	NetAddress(final String hostString, byte[] byteIPAddr, final int port,
			final CB1<AddressIF> cbAddress) {
		log.debug("hostString=" + hostString + " byteIPAddr=" + POut.toString(byteIPAddr) + " port="
				+ port);
		this.byteIPAddr = byteIPAddr;
		this.port = port;

		if (hostString != null)
			parseHostString(hostString);

		if ((this.port <= 0) || (this.port > 65535)) {
			log.debug("Missing or invalid port: " + port + " with hostString=" + hostString);
			this.port = UNKNOWN_PORT;
		}

		// Is this an asynchronous call with DNS resolution?
		if (cbAddress != null) {
			resolve(new CB0() {

				protected void cb(CBResult result) {
					switch (result.state) {
						case OK: {
								try {
									if (NetAddress.this.port != UNKNOWN_PORT) {									
										NetAddress.this.inetSocketAddress = new InetSocketAddress(
												InetAddress.getByAddress(NetAddress.this.byteIPAddr), NetAddress.this.port);										
									} else {
										log.debug("Created address without port: hostname=" + hostname);
										NetAddress.this.inetAddress = InetAddress.getByAddress(hostname, NetAddress.this.byteIPAddr);
									}										
								} catch (UnknownHostException e) {
									String ip = null;
									if (NetAddress.this.byteIPAddr != null)
										ip = NetUtil.byteIPAddrToString(NetAddress.this.byteIPAddr);
									log.error("Resolved address incorrectly. host=" + hostString + " ip=" + ip
											+ " port=" + NetAddress.this.port + ": " + e);
								}
							break;
						}
						case TIMEOUT:
						case ERROR: {
							log.debug("Could not resolve address=" + hostname + " result=" + result);
							break;
						}
					}
					cbAddress.call(result, NetAddress.this);
				}
			});
		} else {

			if (this.byteIPAddr == null) {
				log.debug("No IP address found.");
				this.unresolvable = true;
				return;
			}
			
			AddressFactory.getDNSComm().updateCache(hostname, this.byteIPAddr);

			try {
				log.debug("NetUtil.byteIPToIntIP(this.byteIPAddr)="
						+ NetUtil.byteIPToIntIP(this.byteIPAddr));
				log.debug("byteIPAddr=" + POut.toString(this.byteIPAddr));
				// Is this the wildcard address?
				if (NetUtil.byteIPToIntIP(this.byteIPAddr) != 0) {
					if (port != UNKNOWN_PORT) {					
						this.inetSocketAddress = new InetSocketAddress(InetAddress.getByAddress(this.byteIPAddr),
							this.port);
						log.debug("inetSocketAddress=" + inetSocketAddress);
					} else {
						this.inetAddress = InetAddress.getByAddress(hostname, byteIPAddr);						
					}						
				} else {					
					log.info("Creating a server socket with a wildcard address");
					this.isWildcardAddress = true;
					this.inetSocketAddress = new InetSocketAddress(this.port);
				}
			} catch (UnknownHostException e) {
				log.error("This should never happen: " + e);
			}
		}
	}

	private void parseHostString(String hostString) {
		// log.debug("Parsing hostString=" + hostString);
		String hostPart = null;
		if (hostString.contains(":")) {
			String[] splitString = hostString.split(":");
			hostPart = splitString[0];

			// Set the port number
			String portString = splitString[1];
			this.port = Integer.valueOf(portString);

		} else {
			hostPart = hostString;
		}

		if (hostPart.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
			// This is an IP address
			this.byteIPAddr = NetUtil.stringIPToByteIP(hostPart);
			log.debug("Found an IP address=" + hostPart + " byteIPAddr=" + POut.toString(this.byteIPAddr));
			 log.debug("String IP=" + NetUtil.byteIPAddrToString(this.byteIPAddr));

			// Is this the local address?
			if (hostPart.equals("127.0.0.1")) {
				this.hostname = "localhost";
			}

		} else {
			// This is a hostname
			this.hostname = hostPart;

			// Is this the local host?
			if (hostname.toLowerCase().equals("localhost")) {
				log.debug("Localhost address found");
				this.byteIPAddr = NetUtil.stringIPToByteIP("127.0.0.1");
			}
		}
	}

	private void resolve(final CB0 cb) {
		if (hostname == null && byteIPAddr != null) {

			// Do a reverse DNS lookup
			AddressFactory.getDNSComm().getNameByIPAddr(byteIPAddr, new CB1<String>() {

				protected void cb(CBResult result, String hostname) {
					switch (result.state) {
						case OK: {
							log.debug("Hostname resolved: " + NetUtil.byteIPAddrToString(byteIPAddr) + "->"
									+ hostname);
							NetAddress.this.hostname = hostname;
							cb.call(result);
							break;
						}
						case TIMEOUT:
						case ERROR: {
							log.warn("Could not reverse resolve ip=" + NetUtil.byteIPAddrToString(byteIPAddr));
							cb.call(CBResult.OK());
							break;
						}
					}
				}
			});
		} else if (hostname != null && byteIPAddr == null) {

			// Do a DNS lookup
			AddressFactory.getDNSComm().getIPAddrByName(hostname, new CB1<byte[]>() {

				protected void cb(CBResult result, byte[] byteIPAddr) {
					switch (result.state) {
						case OK: {
							log.debug("Address resolved: " + hostname + "->"
									+ NetUtil.byteIPAddrToString(byteIPAddr));
							NetAddress.this.byteIPAddr = byteIPAddr;
							cb.call(result);
							break;
						}
						case TIMEOUT:
						case ERROR: {
							log.warn("Could not resolve host=" + hostname);
							cb.call(result);
							break;
						}
					}
				}
			});
		} else {
			// Nothing to do
			EL.get().registerTimerCB(new CB0() {
				protected void cb(CBResult result) {
					cb.call(CBResult.OK());
				}
			});
		}
	}

	public String getHostname() {
		if (hostname == null)
			return (byteIPAddr != null) ? NetUtil.byteIPAddrToString(byteIPAddr) : "?";
		else
			return hostname;
	}

	public int getPort() {
		return port;
	}
	
	public boolean hasPort() {
		return port != UNKNOWN_PORT;
	}

	public byte[] getByteIPAddr() {
		return byteIPAddr;
	}

	public int getIntIPAddr() {
		return NetUtil.byteIPToIntIP(byteIPAddr);
	}

	public InetSocketAddress getInetSocketAddress() {
		assert port != UNKNOWN_PORT : "Tried to access a socket addr without a port number. hostname="
				+ hostname;
		assert inetSocketAddress != null : "Tried to access an address that could not be resolved. hostName="
				+ hostname;
		return inetSocketAddress;
	}
	
	public InetAddress getInetAddress() {
		assert inetSocketAddress != null || inetAddress != null;		
		return inetSocketAddress != null ? inetSocketAddress.getAddress() : inetAddress;		
	}


	public byte[] getByteIPAddrPort() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.put(byteIPAddr);
		bb.putInt(port);
		return bb.array();
	}

	public boolean isWildcard() {
		return isWildcardAddress;
	}

	public boolean isResolved() {
		return (byteIPAddr != null);
	}

	/*
	 * @see edu.harvard.syrah.sbon.communication.AddressIF#equals(edu.harvard.syrah.sbon.communication.AddressIF)
	 */
	public boolean equals(Object obj) {
		assert obj instanceof NetAddress : "Cannot compare NetAddress to non-NetAddress.";

		NetAddress cmpAddress = (NetAddress) obj;

		if (unresolvable)
			return hostname.equals(cmpAddress.hostname);

		return byteIPAddr[0] == cmpAddress.byteIPAddr[0] && byteIPAddr[1] == cmpAddress.byteIPAddr[1]
				&& byteIPAddr[2] == cmpAddress.byteIPAddr[2] && byteIPAddr[3] == cmpAddress.byteIPAddr[3]
				&& port == cmpAddress.port;
	}

	public int compareTo(Object obj) {
		assert obj instanceof NetAddress : "Cannot compare NetAddress to non-NetAddress.";

		NetAddress cmpAddress = (NetAddress) obj;
		if (unresolvable)
			return hostname.compareTo(cmpAddress.hostname);

		for (int i = 0; i < 4; i++) {
			if (byteIPAddr[i] < cmpAddress.byteIPAddr[i])
				return -1;
			if (byteIPAddr[i] > cmpAddress.byteIPAddr[i])
				return 1;
		}
		return 0;
	}

	public int hashCode() {
		if (unresolvable)
			return hostname.hashCode();

		assert byteIPAddr != null : "byteIPAddr=null hostname=" + hostname;
		return byteIPAddr[0] + byteIPAddr[1] + byteIPAddr[2] + byteIPAddr[3] + port;
	}

	public String toString() {
		return toString(true);
	}

	public String toString(boolean colour) {
		String ipStr = (byteIPAddr != null) ? NetUtil.byteIPAddrToString(byteIPAddr) : "?";
		// String outputHostname = (hostname == null) ? "/" + ipStr : hostname + "/"
		// + ipStr;
		String outputHostname = (hostname == null) ? "/" + ipStr : hostname; // +
																																					// "/"
																																					// +
																																					// ipStr;

		String addPort = (port != UNKNOWN_PORT) ? ":" + port : "";

		if (colour)
			return ANSI.color(ADDR_COLOR, outputHostname + addPort);

		return outputHostname + addPort;
	}

	public static void main(String[] args) {		
		EL.set(new EL(0, false));

		ANSI.use(true);

		String[] oneAddr = args;
		
	   NetAddress ip = ( NetAddress )AddressFactory.createUnresolved("202.197.22.56", 16);
	   int b = NetUtil.serializeObject(ip).length;
	   System.out.println(b);
	   
	   int addrInt=ip.getIntIPAddr();
	   System.out.println(addrInt);
	   
	   byte[] addr2 = NetUtil.intIPToByteIP(Integer.valueOf(addrInt));
	   
	   System.out.println(NetUtil.byteIPAddrToString(addr2)+", "+addr2.length);
	   
	   System.out.println(NetUtil.byteIPAddrToString(ip.getByteIPAddr()));
	   
	   System.out.println(NetUtil.byteIPAddrToString(ip.getByteIPAddr()));
	   
	   
		/*
		String[] fewAddr = new String[] { "www.leo.org:80", "www.yahoo.com:80",
				"ypsilantigreens.blogspot.com:80", "iowageek.blogspot.com:80",
				"speedmerchant.blogspot.com:80", "www.cl.cam.ac.uk:80", "speedmerchant.blogspot.com:80",
				"speedmerchant.blogspot.com:80", "www.amazon.com:80", "asdsd.asdsadsa.asdsad.asdsa:80",
				"www.jambo.net:80", "blogs.codehaus.org:80", "greatgodpan.com:80", "www.nytimes.com:80",
				"www.cartoonchurch.com:80", "feeds.feedburner.com:80", "farneweblog.com:80",
				"larvalife.blogspot.com:80", "larvalife.blogspot.com:80", "blogs.sun.com:80",
				"www.rklau.com:80", "www.slashdot.org:80" };

		String[] manyAddr = new String[] { "ypsilantigreens.blogspot.com:80",
				"speedmerchant.blogspot.com:80", "ingchingcrazycorner.blogspot.com:80",
				"moshiachblog.blogspot.com:80", "sleepdreaming.blogspot.com:80",
				"sigurdtosigurd.blogspot.com:80", "mygaymarriage.blogspot.com:80",
				"loualfeld.blogspot.com:80", "siewyee.blogspot.com:80", "lisasamson.blogspot.com:80",
				"theriverspeaks.blogspot.com:80", "globalocal.blogspot.com:80",
				"larvalife.blogspot.com:80", "mlmfuture.blogspot.com:80", "megaherzeleid.blogspot.com:80",
				"leftinthereign.blogspot.com:80", "bigfigure.blogspot.com:80",
				"imdebsnaps.blogspot.com:80", "louisvillemark.blogspot.com:80", "IowaGeek.blogspot.com:80",
				"hungryhyaena.blogspot.com:80", "soulsalliance.blogspot.com:80",
				"cornandpork.blogspot.com:80", "dmcc.blogspot.com:80", "doggieger.blogspot.com:80",
				"kindredpain.blogspot.com:80", "afreevietnam.blogspot.com:80",
				"bulldogbadger.blogspot.com:80", "kymc.blogspot.com:80", "thehermitess.blogspot.com:80",
				"timmyaffil.blogspot.com:80", "prabato.blogspot.com:80", "downingstreet.blogspot.com:80",
				"feveredrants.blogspot.com:80", "goonblog.blogspot.com:80",
				"wingsofeaglesmin.blogspot.com:80", "politicalquestion.blogspot.com:80",
				"padup.blogspot.com:80", "mindlessinottawa.blogspot.com:80", "chrth.blogspot.com:80",
				"goatlogic.blogspot.com:80", "blueyedtracy.blogspot.com:80",
				"circumlocute.blogspot.com:80", "japanpotato.blogspot.com:80",
				"injudiciousgardening.blogspot.com:80", "englfbatista.blogspot.com:80",
				"kendblog.blogspot.com:80", "deidreknight.blogspot.com:80",
				"islanduniverse2065.blogspot.com:80", "lankachildsupport.blogspot.com:80",
				"allianceofpower.blogspot.com:80", "tweakandbeat.blogspot.com:80",
				"ebookgroup.blogspot.com:80", "prumble.blogspot.com:80", "macgenie.blogspot.com:80",
				"welcomematt.blogspot.com:80", "knittingchick12.blogspot.com:80",
				"atlmmim.blogspot.com:80", "janestarr.blogspot.com:80", "ericsimonson.blogspot.com:80",
				"blacksheeppress.blogspot.com:80", "stirfriedheart.blogspot.com:80",
				"carinmincemoyer.blogspot.com:80", "attemptingescape.blogspot.com:80",
				"trondant.blogspot.com:80", "sanchar.blogspot.com:80", "neveragainmichel.blogspot.com:80",
				"rgsspecialists.blogspot.com:80", "iamshahid.blogspot.com:80",
				"unobserver.blogspot.com:80", "zzonkedd.blogspot.com:80", "jasonjenny.blogspot.com:80",
				"addaboy.blogspot.com:80", "tonym1203.blogspot.com:80",
				"vancouvercanuckshockey.blogspot.com:80", "crumblehall.blogspot.com:80",
				"blogofdisquietude.blogspot.com:80", "katemcdonald.blogspot.com:80",
				"sharedchanges.blogspot.com:80", "juliepede.blogspot.com:80",
				"asilentcacophony.blogspot.com:80", "mogageek.blogspot.com:80",
				"fireblazt.blogspot.com:80", "reflectingtheglory.blogspot.com:80",
				"rfseen.blogspot.com:80", "confrontingtheculture.blogspot.com:80",
				"lamontestamps.blogspot.com:80", "typenoevil.blogspot.com:80",
				"eccentricpat.blogspot.com:80", "the40s.blogspot.com:80", "justgina.blogspot.com:80",
				"windmillspinner.blogspot.com:80", "apancado.blogspot.com:80",
				"dyingtouse.blogspot.com:80", "greatebooks3.blogspot.com:80",
				"greatebooks4.blogspot.com:80", "cgamg.blogspot.com:80", "viciouspancake.blogspot.com:80",
				"79432.blogspot.com:80", "swirlymuffins.blogspot.com:80", "johnnyawesomo.blogspot.com:80",
				"scra.blogspot.com:80", "abusinan.blogspot.com:80", "mumpsimus.blogspot.com:80",
				"wwjd5.blogspot.com:80", "andyatkinskruger.blogspot.com:80", "wallywatch.blogspot.com:80",
				"furtherramblings.blogspot.com:80", "bemuseme.blogspot.com:80",
				"achipofftheoldblog.blogspot.com:80", "walrusomnibus.blogspot.com:80",
				"lawbot.blogspot.com:80", "stjacques.blogspot.com:80", "andrewvis.blogspot.com:80",
				"dissonantculture.blogspot.com:80", "kadnine.blogspot.com:80",
				"caipirinhasinenglish.blogspot.com:80", "truckerphilosophy.blogspot.com:80",
				"jreflect.blogspot.com:80", "bettysutility.blogspot.com:80", "anytownusa.blogspot.com:80",
				"geekymom.blogspot.com:80", "livetoad.blogspot.com:80", "myfearlessbounce.blogspot.com:80",
				"saddamhussein.blogspot.com:80", "forensicsandfaith.blogspot.com:80",
				"mysteryachievement.blogspot.com:80", "carpeimperiummundo.blogspot.com:80",
				"lizholmes.blogspot.com:80", "angryamerican.blogspot.com:80",
				"jaybradfield.blogspot.com:80", "desiyah.blogspot.com:80", "allupinhere.blogspot.com:80",
				"chiho103.blogspot.com:80", "disciplerefuge.blogspot.com:80", "coolpri.blogspot.com:80",
				"scholasticum.blogspot.com:80", "newyorkerintokyo.blogspot.com:80",
				"trustysteed.blogspot.com:80", "diaryofrobinkathleen.blogspot.com:80",
				"gaskinbalrog.blogspot.com:80", "xtines.blogspot.com:80", "sfchroniclebiz.blogspot.com:80",
				"haystacks37643.blogspot.com:80", "avirandomthots.blogspot.com:80",
				"ximebangalore.blogspot.com:80", "ronanj.blogspot.com:80", "sketchy1.blogspot.com:80",
				"drhanleydocs.blogspot.com:80", "jemappellesam.blogspot.com:80",
				"londonmark.blogspot.com:80", "loveyeshua.blogspot.com:80", "grecianed.blogspot.com:80",
				"somethingtolookforwardto.blogspot.com:80", "hawkesmorenomore.blogspot.com:80",
				"holycommunion.blogspot.com:80", "maskedmoviesnobs.blogspot.com:80",
				"politzero.blogspot.com:80", "jimbuck2.blogspot.com:80", "twiceaheretic.blogspot.com:80",
				"mvbarer.blogspot.com:80", "sandflies.blogspot.com:80", "csunmod.blogspot.com:80",
				"fallujapictures.blogspot.com:80", "vatsaview.blogspot.com:80",
				"schaferspoetry.blogspot.com:80", "asubmissivejourney.blogspot.com:80",
				"themainthing.blogspot.com:80", "ctenuchid.blogspot.com:80", "victro.blogspot.com:80",
				"thereportcard.blogspot.com:80", "littledivinities.blogspot.com:80",
				"rk77.blogspot.com:80", "ncsteve.blogspot.com:80", "kohoco.blogspot.com:80",
				"magicadvocate.blogspot.com:80", "thedevilsplaything.blogspot.com:80",
				"zubari.blogspot.com:80", "entiatalk.blogspot.com:80",
				"dennismccowantheviewfrommissouri.blogspot.com:80", "futenma2.blogspot.com:80",
				"netajimystery.blogspot.com:80", "mainepolitics.blogspot.com:80",
				"crawlingoffthealtar.blogspot.com:80", "meditativerose.blogspot.com:80",
				"worldmissions.blogspot.com:80", "ionicus.blogspot.com:80", "barisaxyvet.blogspot.com:80" };
				
		*/

		final List<String> addrList = new ArrayList<String>();

		for (String a : oneAddr)
			addrList.add(a);

		final int totalNum = addrList.size();

		log.main("addrList.size=" + totalNum);

		EL.get().registerTimerCB(new CB0() {
			protected void cb(CBResult result) {
				AddressFactory.createResolved(addrList, new CB1<Map<String, AddressIF>>() {

					protected void cb(CBResult result, Map<String, AddressIF> addressMap) {
						switch (result.state) {
							case OK: {
								log.main("Got addressMap.keyset.size=" + addressMap.keySet().size());
								for (String addrStr : addressMap.keySet()) {
									AddressIF addr = addressMap.get(addrStr);
									log.main("address=" + addrStr + " resolvedAddr=" + addr + " address.byteIP="
											+ NetUtil.byteIPAddrToString(((NetAddress) addr).getByteIPAddr()));
								}
								log.main("missing=" + (totalNum - addressMap.keySet().size()));
								break;
							}
							case ERROR: {
								log.error("result.what=" + result.what);
								break;
							}
						}
					}
				});
			}
		});

		/*
		EventLoop.get().registerTimerCB(100, new EventCB() {
			protected void cb(CBResult result, Event arg1) {
				EventLoop.get().exit();
			}						
		});
		*/ 
		
		EL.get().main();
	}

}
