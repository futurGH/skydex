module default {
    type Post {
        required uri: str { constraint exclusive; };
        index on (.uri);
        required cid: str;
        required createdAt: datetime;
        index on (.createdAt);

        required author: User;
        required text: str;
        embed: Embed;
        altText: str;

        parent: Post;
        root: Post;
        quoted: Post;

        required multi likes: User {
            rkey: str;
        };
        required multi reposts: User;
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

        required multi followers: User;
        multi following := .<followers;
    }
}
