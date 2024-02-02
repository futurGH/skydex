module default {
    type Post {
        required uri: str { constraint exclusive; };
        index on (.uri);
        required cid: str;
        required createdAt: datetime;
        index on (.createdAt);

        required author: User { on target delete delete source; };
        required text: str;
        embed: Embed;
        altText: str;

        parent: Post { on target delete allow; };
        root: Post { on target delete allow; };
        quoted: Post { on target delete allow; };

        required multi likes: User {
            rkey: str;
            on target delete allow;
        };
        required multi reposts: User {
            rkey: str;
            on target delete allow;
        };
        multi replies := .<parent;

        multi langs: str;
        multi tags: str;
        multi labels: str;
    }

    type Embed {
        title: str;
        description: str;
        uri: str;
    }

    type User {
        required did: str { constraint exclusive; };
        index on (.did);
        required handle: str { constraint exclusive; };
        index on (.handle);

        required displayName: str;
        required bio: str;

        required multi followers: User {
            on target delete allow;
        };
        multi following := .<followers;
    }
}
