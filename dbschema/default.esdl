module default {
    type Post {
        required uri: str { constraint exclusive; };
        index on (.uri);
        required cid: str;
        required createdAt: datetime;
        index on (.createdAt);

        required author: User { on target delete delete source; };
        required text: str;
        embed: json;
        altText: str;

        parent: Post { on target delete allow; };
        root: Post { on target delete allow; };
        quoted: Post { on target delete allow; };

        multi likes: User {
            rkey: str;
            on target delete allow;
        };
        multi reposts: User {
            rkey: str;
            on target delete allow;
        };
        multi replies := .<parent;

        multi langs: str;
        multi tags: str;
        multi labels: str;
    }

    type User {
        required did: str { constraint exclusive; };
        index on (.did);
        required handle: str { constraint exclusive; };
        index on (.handle);

        required displayName: str;
        required bio: str;

        multi followers: User {
            rkey: str;
            on target delete allow;
        };
        multi following := .<followers;
    }
}
