package org.rascalmpl.library.experiments.resource.results.buffers;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.IValueFactory;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.values.ValueFactoryFactory;

public class TwitterSearchFiller implements ILazyFiller {

	private String queryString;
	private String lang;
	private TypeStore ts;
	private String refillString;
	private boolean morePages;
	
	public TwitterSearchFiller(String queryString, String lang, TypeStore ts) {
		this.queryString = queryString;
		this.lang = lang;
		this.ts = ts;
		this.refillString = null;
		this.morePages = true;
	}

	@Override
	public IValue[] refill(int pageSize) {
		try {
			String thisQuery = null;
			if (refillString != null) {
				thisQuery = "http://search.twitter.com/search.json" + refillString;
			} else if (morePages) {
				thisQuery = "http://search.twitter.com/search.json?rpp=" + Integer.toString(pageSize) + "&q=" + URLEncoder.encode(queryString,"UTF-8");
				if (lang != null) thisQuery = thisQuery + "&lang=" + URLEncoder.encode(lang,"UTF-8");
			}
			
			URL url = new URL(thisQuery);
			InputStream istream = url.openStream();
			StringBuilder sb = new StringBuilder();
			
			byte[] buf = new byte[4096];
			int count;
	
			while((count = istream.read(buf)) != -1){
				sb.append(new java.lang.String(buf, 0, count));
			}
		
			istream.close();
			
			String res = sb.toString();
			
			ObjectMapper mapper = new ObjectMapper();
			Map<String,Object> twitterData = mapper.readValue(res, Map.class);
			if (!twitterData.containsKey("error")) {
				refillString = (String)twitterData.get("next_page");
				if (refillString == null) morePages = false;
				ArrayList<Object> results = (ArrayList<Object>)twitterData.get("results");
				ArrayList<IValue> vresults = new ArrayList<IValue>(pageSize);
				IValueFactory vf = ValueFactoryFactory.getValueFactory();
				for (Object o : results) {
					Map<String,Object> rmap = (Map<String,Object>)o;
					String text = (String)rmap.get("text");
	//				String geoCode = (String)rmap.get("geo");
					String isoLanguageCode = (String)rmap.get("iso_language_code");
					String toUserName = (String)rmap.get("to_user_name");
					Integer toUserId = (Integer)rmap.get("to_user_id");
					String toUserIdString = (String)rmap.get("to_user_id_str");
					String source = (String)rmap.get("source");
					Integer fromUserId = (Integer)rmap.get("from_user_id");
					String fromUserIdString = (String)rmap.get("from_user_id_str");
					String fromUser = (String)rmap.get("from_user"); 
					String createdAt = (String)rmap.get("created_at");
					String toUser = (String)rmap.get("to_user");
					String idStr = (String)rmap.get("id_str");
					String profileImageUrl = (String)rmap.get("profile_image_url");
					IValue resValue = vf.tuple(vf.string(fromUser),vf.string(text));
					vresults.add(resValue);
				}
				
				IValue[] resultArray = new IValue[vresults.size()];
				for (int idx = 0; idx < vresults.size(); ++idx) resultArray[idx] = vresults.get(idx);
				return resultArray;
			}
		} catch (MalformedURLException murle) {
			
		} catch (IOException ioe) {
			
		}
		
		return new IValue[0];
	}

	@Override
	public ILazyFiller getBufferedFiller() {
		return new TwitterSearchFiller(queryString, lang, ts);
	}

}
